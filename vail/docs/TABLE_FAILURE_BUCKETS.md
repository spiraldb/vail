# Hail Table failure buckets

These buckets classify the 99 failing Table-only definitions from the fresh support run. Each definition belongs to one bucket according to its first observed failure; resolving that failure may expose further missing functionality. The 18 blocked definitions are excluded.

| Bucket | Failing definitions |
|---|---:|
| Collection, display, and localization actions | 12 |
| Randomness and sampling | 12 |
| Compatibility utilities and harness imports | 11 |
| Pandas conversion | 10 |
| Expression helpers and field metadata | 9 |
| Value construction, type inference, and ndarray construction | 9 |
| Indexed reads and write options | 6 |
| Interval lookups, all-match lookups, and interval filtering | 6 |
| Direct table queries | 5 |
| Cache, persist, and checkpoint | 4 |
| Grouped collection and aggregation options | 4 |
| Ordered scans | 4 |
| Partition-count semantics | 4 |
| Exception types and error messages | 2 |
| Import regex filtering and replacement | 1 |

Compatibility utilities include missing Vail helpers and two explicit imports of Hail inside upstream tests. They are not necessarily missing execution-engine functionality. Partition-count assertions and exception-message mismatches are behavioral gaps rather than missing operators.

[Per-test classification and first errors](../conformance/runs/20261002T001552Z-table-only-status/failure-buckets.json).
