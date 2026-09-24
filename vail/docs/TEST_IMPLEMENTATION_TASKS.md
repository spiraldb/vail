# Implementation work behind the Hail Table tests

The [per-test inventory](HAIL_TEST_INVENTORY.md) and [machine-readable rows](../conformance/hail-test-inventory.json) retain the original tests and their dependencies. This document explains the implementation required by their work-package labels. A test can depend on several packages; completing a native primitive does not close a test that also needs expressions, persistence, fixtures or a Python adapter.

## Dispatched bounded tasks

| Task | Agent/model | Concrete deliverable | Upstream coverage and limits |
| --- | --- | --- | --- |
| Inventory | `table_test_inventory` / GPT-6 Luna | Reproducible Python/Scala test inventory with per-test requirements, parameterization, source references and explicit scope review | Static audit; no inferred pass from similar tests |
| T1 tail and partition slice | `table_tail_partition_slice` / GPT-6 Sol | Native range construction, ordered head/tail, forced count, partition introspection, primitive field collection and schema metadata | Original range-table metadata/zero/large-partition tests and `rt`/`par` head/tail variants; cache/checkpoint remain open |
| S2 primitive fragment planner | `vortex_iceberg_storage` / GPT-6 Sol | Typed composite-key interval dictionary, metadata ordering, inclusive/exclusive range selection, snapshot-binding validation | Implements a native prerequisite of indexed reads; no claim that Python indexed-read/query tests pass |
| T0 constructor argument validation | `constructor_argument_validation` / GPT-6 Luna | Hail's mutually exclusive `schema`/`partial_type` validation, with exact error category/message | One specific original constructor-error test; partial inference remains unsupported |
| Independent inventory review | `table_tail_partition_slice` / GPT-6 Sol, after T1 | Audit helper/fixture-backed Table cases outside dedicated suites and coverage/classification errors | Read-only review; separate from implementation claims |
| Expression test requirements | `table_tail_partition_slice` / GPT-6 Sol | Source-reviewed requirements for 100 candidates in five expression modules | Distinguishes shared expression semantics, Matrix dependencies and two false-positive Matrix cases |
| Core/interval test requirements | `constructor_argument_validation` / GPT-6 Luna | Source-reviewed requirements for 31 interval, genomic-range, IR and randomness candidates | Retains fixture provenance and distinguishes public behavior from internal IR assertions |
| Native regression requirements | `table_test_inventory` / GPT-6 Luna | Source-reviewed requirements for all 215 Scala candidates, including 77 core Table/order cases and 138 optimizer/shared/backend cases | Native test translation remains required; no original Scala execution claimed |
| Other Python requirements | `table_tail_partition_slice` / GPT-6 Sol | Source-reviewed requirements for 160 remaining I/O, scientific-method, filesystem, backend and related candidates | Algorithms and Matrix/BlockMatrix boundaries are explicit |
| Matrix/VDS/plot boundaries | `constructor_argument_validation` / GPT-6 Luna, integrator review | Source-reviewed requirements for 138 candidates in those modules, including Table-only cases inside Matrix suites | One helper-backed VDS case was added during final integration; every candidate has a scope disposition |

The reviewed integration now has 62 passing local Python/harness tests, 25 passing Rust tests, and passing original Hail select/transmute and filter bodies. The two Luna implementation tasks delivered native row filtering and array construction/indexing/slicing/reductions. Nine recorder checks and 19 Python subtests also pass. Full upstream annotate remains failing; see [the test report](TABLE_EXPRESSION_TEST_RESULTS.md). New delegations should use Luna by default, with Sol reserved for critical work.

Each implementation task has one owner. The root agent integrates and runs source-extracted upstream cases. No Astra subagents are used. Remaining work below is a dependency queue, not a claim that additional agents are running.

## T0 — Construction, schema and row transformation

`parallelize` must accept Hail schema objects/strings and typed array expressions, infer compatible types across all rows, honor partial type constraints, and distinguish missing values from missing fields and empty collections. Nested structs, arrays, sets, dictionaries, tuples, ndarrays and genomic types require native representations and lossless Python transport. Preserve schema field order and row/global axes, key field identity, empty rows and zero-column row counts.

`select`, `annotate`, `drop`, `rename`, `transmute`, `flatten` and `expand_types` need source-checked typed expressions. Preserve keys or reject forbidden key mutation according to Hail; rename keys and source indices consistently; reject duplicate destinations and row/global name collisions. Transmute removes referenced fields without losing newly assigned results. Nested selection and flattening need stable field identity, not position-only lookup. Examples: `test_annotate`, `test_select`, `test_drop`, `test_rename`, `test_transmute_key`, `test_flatten`, `test_expand_types_on_all_types`, and wide-table tests.

The billion-row range test needs lazy native range batches and an int64 sum path; eager construction cannot satisfy it. The current constructor is native but still materializes its batches. pandas/Spark ingestion belongs to the interop package as well as schema conversion.

## T1 — Actions, truncation and logical partitions

`count`, `_force_count`, `collect`, `take`, `head` and `tail` must respect the operation sequence. Applying a truncation before sorting cannot be rewritten to truncation after sorting. `_force_count` evaluates the plan, whereas a metadata count may avoid evaluation when justified. Preserve primitive and nested missing values through action results. Expression collection and `_localize=False` return expression objects, not ordinary Python lists.

`n_partitions`, `_filter_partitions`, repartition/coalesce and partition-local operations require logical partition state distinct from worker count. Hail range construction uses `max(min(n, requested_partitions), 1)` and distributes remainder rows to earliest partitions; `parallelize` has a separate distribution rule. Key-changing operations must preserve the appropriate public partition contract; the prototype's global sort currently collapses keyed parallelization to one partition, a remaining gap. Cache/checkpoint branches of `test_table_head_and_tail` additionally require actual persistence behavior.

## T2 — Key comparison, explicit order and partitioning

Implement the versioned Hail comparator, equality and hash contracts separately. Cover missing values, null placement in descending order, NaN/infinity/signed zero, Unicode ordering, nested lexicographic comparison and genomic contig order. Computed keys and `order_by` expressions must be evaluated with source checks; `order_by` removes the logical key while retaining explicit row-order requirements. Duplicate keys are not uniqueness guarantees and generally do not impose stable tie order.

Range partitioning, prefix-key changes and physical key truncation must propagate valid proofs through projections, filters, joins and exchanges. A keyed storage read starts with ordered fragments and must not introduce a whole-table row sort. The current primitive in-memory sort is appropriate for order-establishing operations, not a replacement for the storage index. Examples: `test_take_order`, `test_order_by_expr`, `test_table_order_by_head_rewrite`, null-key join tests, and the Scala RVD partitioner/order-lowering suites.

## T3 — Globals and expression source validation

Carry typed globals as native Table metadata through every transformation, join, union, write/read and checkpoint. Implement `annotate_globals`, `select_globals`, `transmute_globals`, `index_globals`, `aggregate_globals` and evaluation of global expressions. Enforce row/global source axes, prohibit stale or foreign references, and implement Hail's conflict/precedence rules. An empty `globals.dtype` property alone does not provide global-expression support. Examples: `test_index_globals`, `test_transmute_globals`, `test_multi_way_zip_join_globals`, `test_group_by_aggregate_doesnt_clobber_global_field`.

## T4 — Filters and interval predicates

Boolean predicates need Hail missingness semantics: a missing predicate discards its row, including when `keep=False`; it is not a Python truth-value coercion. Implement source checking and preserve surviving row order. Interval filters and predicates on key prefixes must use typed endpoints, inclusivity and missing values consistently, pruning fragments before reading rows when the key index supports them. Extraction must recognize safe compound predicates without changing semantics. Tests under `extract_intervals/` often obtain `ht` through parent fixtures rather than constructing a Table in the body.

## T5 — Scalar expressions, aggregators, grouping and scans

This label contains several dependent implementation tasks, not one monolithic assignment:

1. A shared typed expression IR and source/axis checker, native primitive literals/operators/casts/conditionals, exact missingness, numeric overflow/error behavior, and native result transport. Python only constructs expressions.
2. Nested collections, structs, higher-order array/stream operations, sets/dictionaries, ndarray operations and genomic/reference functions used by Table expressions. Local `hl.eval` tests may be shared dependencies without being Table-only tests.
3. Scalar and grouped aggregators with init/update/combine/finalize contracts: count, sum, stats, collect/collect-as-set, take/take-by, array aggregation, counters and the specialized aggregators actually referenced by a test. Preserve empty/all-missing behavior and supported numerical reproducibility.
4. Exclusive ordered scans, including partition carry and interaction with filter/head/tail, and `add_index`. Stateful, random and floating-point scans cannot be treated as arbitrary SQL windows.
5. `_map_partitions`, `group_within_partitions` and stream combinators, with source/key safety and group-boundary handling. Their private APIs must remain explicitly classified.
6. Seeded randomness, row/partition identity and UID propagation through each tested Table IR operation. Determinism and uniqueness tests depend on semantic identity propagation, not merely a random-number function.

Examples include `test_filter_missing`, `test_scan_filter`, `test_scan_tail`, all three GroupedTable tests, `test_keys_before_scans`, `test_group_within_partitions*`, and the `test_table_randomness_*` family. Mixed Matrix-derived randomness tests need a separate boundary decision or Table-only fixture.

## T6 — Joins, indexing and set-like operations

Implement join kinds with Hail's duplicate multiplicity, null matching, key-prefix behavior, field-name mangling and global/schema rules. Table expression indexing must support exact/prefix keys and the required all-match/interval variants, reject keyless or ill-typed lookup, and preserve source lifetimes. Semi/anti joins differ from ordinary multiplicative joins.

`union` preserves multiplicity and has exact schema/global rules; optional unification may insert missing fields and widen types. `distinct`, `collect_by_key` and `multi_way_zip_join` need their own key/grouping contracts, including skew and empty inputs. Restore or preserve required order through native operator properties. Examples: null-join families, join-types tests, `test_join_with_key_prefix`, `test_multi_way_zip_join*`, `test_union_unify`, `test_distinct` and `test_group_by_key`.

## S2 — Persisted key-interval fragment planning

The [fragment planner](../crates/vail-storage/src/fragment_index.rs) implements only in-memory primitive interval selection. Closing indexed-read tests additionally requires write-side local-sort/boundary validation, immutable persisted index pages, atomic snapshot/index publication, exact live Iceberg task reconciliation, ordered scheduling and key-range row filtering. Prefix keys, loci, nested keys and interval-valued keys need explicit comparators/bound expansion. The primary profile allows touching equal boundaries and concatenates fragments; interior overlap needs a declared bounded merge profile. Missing or invalid required index metadata fails ordinary reads.

`read_table` and `query_table` then need Python/native bindings, schemas/globals/reference metadata, empty and missing-key behavior, interval parameter validation, projection and materialized-expression return types. Tests include `test_indexed_read_1`–`4`, `test_indexed_read_boundaries`, `test_query_table*`, `test_read_partitions*`, and interval-extraction tests. Seven planner unit tests do not constitute end-to-end passes for those original tests.

## T8 — Persistence, I/O and interop

General persistence/export tests are not interval-planner tasks. Implement native Table write/read/checkpoint adapters, explicit overwrite/replace transactions, snapshot metadata and reference roundtrip, local staging/path collision behavior, time travel, cache/persist lifecycle, and `write_many` completion/error semantics. Retain the Vortex reader/writer from Iceberg Rust PR #2835. Legacy `.ht` compatibility and conversion are deferred by user direction. Recreate relevant Table fixtures in the new format; no legacy decoder is part of this work.

Text import/export needs headers, field order, delimiter/escaping, missing markers, compression, parallel part manifests and resource/filesystem adapters. pandas conversion requires dtype rules, nullable integers, nested/flattened columns and ndarray handling. Spark and MatrixTable conversions remain explicit interop boundaries; tests asserting those APIs cannot be silently counted as Table-only passes. Display, summarize, `_same`, tolerance comparison, errors and packaging need separate adapters and test-specific validation.

## T9 — Display and action adapters

Implement exact `show(handler=str)` layout/formatting for nested values, empty and zero-column tables, long field names and width wrapping. `summarize` needs typed summary aggregators. `_same` compares schemas, keys, globals, row multiplicities and values with Hail's numeric tolerance rules. Localized and expression-valued action results require separate adapters; use T1 for the native truncation/count/partition primitives and T5 for expression evaluation.

## T7 — Scala IR and backend test translation

For each Scala test, identify the Table/value IR operations and observable result/error being tested, then map them to the shared native logical IR and execution API. Backend internals such as RVD partitioners, optimizer plan-shape assertions, generated-code/JVM encoding and native Hail readers are not callable unchanged in Rust. Preserve their semantic regression cases as native tests and record when an assertion is implementation-specific or obsolete under the new storage format. A ported scenario is distinct from execution of the original Scala test.

## T10 — Table-only domain helpers

The broader test tree also exercises graph algorithms, interval segmentation and variant transformations using Tables alone. `maximal_independent_set` needs the tested node identity types, source/type validation, tie-break expressions and `keep` semantics. `segment_intervals` needs interval splitting and endpoint/order behavior. Table-based `split_multi_hts` needs genotype and likelihood downcoding, including haploid calls. Preserve compatible Hail frontend compositions where possible and implement the native kernels they require; constructing a Table result alone does not satisfy these tests. Matrix/VDS algorithms remain separately identified boundaries in the supplementary inventory.

## Dispatch gates

The next implementation wave should start with the typed expression/source kernel and write-side index certification, which unlock many test groups. Joins, aggregates, genomic types and RNG should be split further once their prerequisite types/IR are agreed. Tests whose assertions depend on unresolved imported helpers, fixtures, dynamic parameters or a Matrix/Spark boundary stay visible for review. Static inventory labels are not evidence that those dependencies have been executed or that a complete compatibility boundary has been established.
