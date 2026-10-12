# Scan-derived keys in a shared lookup graph

The reported example failed before this fix with `Expects PARTITION BY expression to be ordered` for one partition and `Invalid HashJoinExec, unsupported PartitionMode Auto in execute()` for two partitions. Directly collecting the scan worked.

The DataFusion extension planner receives physical children before physical optimization. Vail's partition extensions converted those children to prepared/deferred private inputs. The outer DataFusion optimizer could not reach those inputs to enforce window sorting/distribution and resolve the scan's compact partition-summary join mode. This was a plan finalization error, not an incorrect scan count.

The extension planner now runs DataFusion's physical optimizer on incoming children with visible operators requiring finalization before encapsulating them. Already executable Vail extension inputs retain their existing partition contracts. DataFusion supplies the required sorting, distribution, and executable join selection; lookup itself continues to use its existing sort-merge join. No rows are collected to perform this optimization. The Table partition wrapper declares SinglePartition for each child because execution selects child partition zero; DataFusion must coalesce any partitions introduced by rewrites before that boundary. Single-partition scan outputs also retain the existing range-restoration extension with zero split points, enforcing one real output partition even if DataFusion parallelizes the upstream windows or summary join.

Regression coverage in `python/tests/test_table_lookup.py` exercises count and sum scans shared by both lookup branches, multiple partition counts, empty/singleton inputs, filtered targets, a scan over a shared-source union, and repeated actions. The original example now returns ordinals 0, 1, 2 and matching rows for both one and two partitions.

The final three-join repeated-key_by probe (4 rows, 2 partitions) completed in 0.313 seconds in the debug build during this run.

Final validation after native rebuild: **744 Python tests passed, 1 skipped, 19 subtests passed; 50 Rust Table tests passed**. Regression additions cover 16 cases. Commands:

```sh
PYTHONPATH=python uv run --no-sync python -m pytest python/tests -q
RUSTC_WRAPPER='' cargo test --offline -j 1 -p vail-table
```

Validation ran against the shared working tree, including the pending scan extensions and P1 fixes. This commit isolates only scan/lookup plan-finalization and partition-contract changes, their tests, and this record. Source hashes identify the tested working-tree files.
