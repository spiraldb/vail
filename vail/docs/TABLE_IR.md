# Lazy TableIR and DataFusion execution

A Python Table wraps a Rust `Arc<TableIR>`. Construction creates immutable, typed
nodes with shared children; it never executes transformations or caches results.
Read construction loads snapshot metadata, while Parallelize owns the caller's
initial Arrow buffers. Range stores bounds. Python builds scalar expression IR;
Rust checks row/global scope, names, types, and key preservation.

Every supported TableIR node lowers to a DataFusion physical execution plan in
`crates/vail-table/src/plan.rs`. There is no separate TableIR interpreter. This is
physical-plan lowering; a DataFusion logical optimizer pipeline is not yet wired in.

| TableIR / action | DataFusion execution |
| --- | --- |
| Read | VicebergScanExec extension, streaming certified fragments |
| Parallelize | ParallelizeExec extension, slicing shared Arrow buffers on demand |
| Range | RangeExec extension, generating bounded batches |
| MapRows / select / annotate / transmute | ProjectionExec with typed physical expressions |
| Filter | FilterExec followed by a projection that removes global bindings |
| MapGlobals | ProjectionExec on the separate singleton globals plan |
| TableGetGlobals / index_globals | Native GlobalsIR reference bound by singleton CrossJoinExec |
| KeyBy / OrderBy | SortExec over its upstream plan, with stable ordinal tie breaking |
| Head | GlobalLimitExec; zero rows lower directly to EmptyExec |
| Tail | OrderedTailExec extension retaining only the requested suffix |
| Forced count | AggregateExec with DataFusion's count aggregate |
| `_same` | Full outer SortMergeJoinExec, HailValuesDiffer, FilterExec, GlobalLimitExec |

The extension operators consume DataFusion child plans and emit Arrow batch streams.
Hail-specific scalar rules (numeric overflow, array bounds, missingness, recursive
value similarity) are physical expressions, not Python row callbacks. Array
construction and length use DataFusion's existing nested functions.

## Ordering and logical partitions

The embedded implementation currently has one ordered DataFusion physical stream.
A private UInt64 column carries Hail logical partition identity through relational
operators. The action adapter removes it and emits partition boundaries for the
storage writer, including empty partitions where Hail preserves them. It checks
that a plan has not reordered those identities. Head/Tail retain only observed,
nonempty logical partitions. Explicit sorting produces one logical partition.
Physical parallelism and a distributed sort remain future work.

VicebergScanExec advertises the certified key order to DataFusion. It follows the
Iceberg manifest entries and Vail fragment interval certificate, opening files as
needed; it never inserts a sort. A prefix action leaves later files unopened.
File hashes are checked before decoding each requested file. Read nodes pin immutable
snapshot metadata, so overwrites do not change an existing Table.

SortExec consumes its source stream directly, with no preceding Rust collection of
the table. An ordinal column preserves arrival order among equal keys and is removed
after sorting. A Rust regression test executes a generated million-row TableIR
under a bounded DataFusion memory pool and verifies actual spill. The default
Python task context has no configured memory limit. Sort keys currently support
integers, booleans, and ASCII strings; full Hail comparison domains remain unfinished.

## Globals

Globals are a separately typed, lazy singleton Arrow relation. The initial relation
has one row and zero fields. `annotate_globals`, `select_globals`, and
`transmute_globals` build projections over it. Scalar IR distinguishes row columns
from global references; global projections reject row-indexed expressions, and
row/global name collisions and incompatible expression sources are rejected.

A row projection or filter binds nonempty globals using DataFusion CrossJoinExec,
with the singleton on its build side. Global values become physical column
references in that plan. They are not collected into a Python dictionary or copied
into every input row at Table construction. Each node references its own child's
globals, so changing a global later does not change an earlier row annotation.

`hl.eval` executes the singleton plan without scanning table rows. Globals therefore
exist independently of row count, including empty Tables and `head(0)`. The current
type system supports primitive numbers, booleans, strings, structs, arrays, and typed
missing values recursively. Sets, dictionaries, tuples, loci, and intervals still
require extensions to the shared type/expression system.

`index_globals()` returns an unindexed expression holding a native `Arc<GlobalsIR>`.
Expression composition preserves that reference outside the JSON wire payload;
there is no Python value collection, serialized table copy, or global registry.
The destination IR captures only the referenced globals, not the source's row IR.
Rust validates the expression's declared type against the captured native schema.
During lowering, each reference becomes a singleton struct projection and a
DataFusion CrossJoinExec. Explicit physical indices keep different tables' globals
separate even when their field names coincide. These bindings work in row
projections, filters, global projections, constructors, and standalone `hl.eval`.
Direct foreign-table field expressions still fail source validation; explicitly
indexing the globals is what makes the reference unindexed. Captured read globals
keep their immutable snapshot after overwrite and after the Python source is freed.

Write evaluates the singleton and stores exactly one Arrow IPC batch in the immutable
generation. Its URI and SHA-256 are bound into the snapshot descriptor, alongside
its ordered logical schema. Reading a Table inspects the schema only; GlobalsScanExec
opens, verifies, and decodes the Arrow file when the globals are evaluated. Empty
globals need no sidecar. This uses Arrow for globals and PR 2835's Vortex/Iceberg
integration for row files; legacy `.ht` compatibility remains deferred.

## Actions and storage

Collect, expression collection, forced count, `_same`, and write execute DataFusion
plans. Exact count/partition metadata can answer eligible metadata-only actions.
Each action builds fresh physical plans; a previous action's results are not cached
inside TableIR. IO and expression errors retain their Python error categories when
propagating through DataFusion or the streaming writer.

Write consumes the executed batch stream with backpressure. Its file buffer flushes
at 64 MiB retained Arrow memory or 1,048,576 rows, and at logical partition boundaries.
An oversized incoming batch may exceed the byte target and is flushed alone. The
row cap splits batches. Rust callers can configure both limits. Only file metadata
and partition counts grow with the full table size; row data is not accumulated by
the coordinator. Sort and tail retain the state required by their respective
operators, independently of the writer's file buffers.

Snapshots publish atomically after all files close. Failure cleans up the new
generation and preserves the prior snapshot. Unkeyed files carry a checked sequence;
keyed files retain the interval certificate, including duplicate and missing key
boundaries. Old generations remain available for pinned readers.

## Table equality

`_same` checks logical row/global schemas and ordered key names before execution.
Field reordering applies only at the top level. Globals are compared by a singleton
DataFusion cross join and Hail's recursive similarity expression, even on empty
Tables.

For rows, a streaming ordinal operator labels each ordered input. A full outer merge
join on those unique ordinals aligns row sequences without multiplying duplicate
keys. The scalar comparison checks keys exactly, values with the requested tolerance,
and presence of both sides. Filtering differences and limiting to one answers the
Boolean action. This reproduces comparison of ordered, collected key groups while
allowing DataFusion to execute the join. A general Python Table.join API is still
unfinished.

A missing tolerance (`None`) follows Hail's missing predicate semantics: compare
key-group presence but ignore values and within-group multiplicity. That case lowers
to distinct-key AggregateExec plans and a full outer HashJoinExec. The unkeyed case
uses a constant grouping key, so empty input still has no group.

The recursive comparison covers supported primitives, structs, arrays, missingness,
NaNs, and signed zero. Float rules follow Hail's `valuesSimilar` and `D_==`, including
the minimum-normal absolute floor and float32 subtraction precision. Diagnostics
identify a differing category; Hail's pretty-printed sample of ten mismatching groups
is not reproduced.

## Validation

Tests cover actual DataFusion plan shapes, laziness with missing files, source
reexecution, shared immutable IR, logical partitions, multi-file prefix reads,
stable duplicate ordering, spilling sort, globals on empty Tables, Arrow globals
persistence, old-snapshot reads, checksum failure, scopes, and recursive `_same`.
The run recorder stores commands, logs, source hashes, and extension hashes under
`conformance/runs`.

Array mapping uses `ArrayMap(value, parameter, body)` and lexically bound
`LambdaVariable` scalar IR nodes. Python invokes the callback once to construct
its typed body; Rust validates each variable against the enclosing map scopes
and infers the result element type. Lowering produces DataFusion's
`HigherOrderFunctionExpr(array_transform)` with `LambdaExpr` and
`LambdaVariable` physical expressions. DataFusion expands captured row/global
columns to the element positions, including captures across nested lambdas.
Missing arrays remain missing, empty arrays remain empty, and missing elements
are passed to the body. Empty/missing arrays do not evaluate the body. No Python
callback runs during execution.

Array `flatmap` composes `ArrayMap` with a typed `ArrayFlatten` node and requires
an array-valued callback. It lowers to DataFusion `array_transform` followed by
`flatten`, preserving element order and flattening exactly one level. Missing
callback arrays contribute no elements; missing outer arrays remain missing.
Before DF flatten, a physical-expression adapter uses DataFusion's Arrow list
normalization to remove value spans underneath null inner lists, whose validity
DF flatten otherwise ignores. This matches Hail's `StreamFlatMap` interpreter
(`hail/hail/hail/src/is/hail/expr/ir/Interpret.scala`) and does not discard missing
individual elements of valid arrays.

Array `filter` has a typed `ArrayFilter(value, parameter, predicate)` node. Its
predicate must be boolean, and it shares lambda binding/lowering with `ArrayMap`.
Execution uses DataFusion `array_filter`; false and missing predicates discard
the element, and surviving elements retain their input order. Lambdas can capture
rows, local globals, external globals, and enclosing lambda variables.

Set literals have a distinct recursive `Set` type (`{"set": element_type}` on
the wire). They use canonical sorted, unique Arrow lists with the reserved list
item field name `__vail_set_item`; ordinary arrays use `item`. A field-name marker
is necessary because DataFusion scalar conversion preserves names but may drop
arbitrary Arrow field metadata. Literal and parallelize ingestion canonicalize
sets by declared type, including missing elements, float32 rounding, NaNs, and
nested sets. Execution remains in DataFusion; this canonicalization prepares
literal/input values, rather than executing table transformations. Viceberg's
logical type descriptor preserves sets across storage's physical list schema.
Python localizes sets and nested sets (frozensets when used as members). Sets
whose members are arrays or structs currently raise an explicit unsupported
localization error; general set operations remain future work.
