# Six Table expression implementations — 2026-10-09

Implemented with GPT-6 Luna subagents and parent code review:

| Requested work | Implementation |
| --- | --- |
| Serial `hl.scan` | DataFusion exclusive-prefix windows; ordered inputs and Hail empty-prefix results. |
| Parallel `hl.scan` | Partition-local windows, one accumulator-state summary per nonempty partition, a summary prefix window, then a DataFusion join and generic state finalization. Real source partitions are preserved, including empty partitions. |
| Array arithmetic | Array/array arithmetic with equal-length validation, scalar broadcasting, numeric promotion, missing propagation, and unary negation through existing expression/DataFusion lowering. |
| Expression-valued interval filters | Unindexed interval-array expressions and mixed Python literal/expression lists, including globals; existing read-fragment pruning retained. |
| Expression-valued dictionaries | Canonical typed keys, missing pair removal, valid missing keys/values, Hail's observed first/last duplicate behavior, using DataFusion collection components. |
| `hl.query_table` | Lazy full-row matching for scalar/global and row-varying point/range queries; compound/prefix/interval keys, all duplicate matches, positional key typing, and missing queries/keys. |

## Validation

- Full Vail Python suite: **681 passed, 1 skipped, 19 subtests passed**.
- Rust `vail-table` and `vail-execution` suites: **104 passed, 0 failed**.
- Final focused dictionary/query review after removing duplicate bound evaluation:
  **40 passed**.
- Targeted original upstream Hail tests: **23 cases passed, 0 failed**, from
  eight test definitions. These are the three scan definitions, four query
  definitions (19 parameterized cases), and `Tests.test_issue_3654`.
- Direct pinned Hail runtime oracle confirms missing point queries match missing
  keys, missing whole intervals yield missing arrays, and missing interval
  endpoints remain valid bound values.
- Direct Hail oracle also confirms the duplicate-key inconsistency: constant
  pair arrays keep the last value; emitted runtime pairs and expression-keyed
  Python mappings keep the first. Constant set-key conversion follows the
  emitted first-value path. Tests cover literal/container, cast, arithmetic,
  collection, row, and global constructor paths.
- Multi-file tests verify constant query pruning and prove table data is read
  lazily by deleting source files after constructing the query expression.

Full-suite logs, targeted upstream result JSON, and Hail oracle scripts/output
are stored in this directory. This is not a rerun of the entire upstream Table
inventory, and these counts must not be reported as the total upstream coverage.

## Remaining limits

- Scan initializers and advanced combinators remain unsupported: histogram and
  approximate quantile/median initializers, genomic/linreg/downsample scan type
  inference, explode, grouped/counter scans, array combinators, and fold. Errors
  are explicit. Supported reducers and filtered-scan behavior are documented in
  [Table scans](../../../docs/TABLE_SCANS.md).
- Row-varying queries read target provider partitions; dynamic per-row fragment
  pruning/index seeks remain a performance improvement. Constant queries prune
  files. See [query execution](../../../docs/QUERY_TABLE.md).
- Randomness, cache/persist, filesystem compatibility helpers, and partial type
  inference in `Table.parallelize` were outside this request.

[Collection details](../../../docs/EXPRESSION_COLLECTIONS.md) describe array and
interval expression lowering and dictionary duplicate resolution.
