# H051: project materialized entry outputs before they are copied

SOURCE/PROOF PROPOSAL ONLY. Base current mainc3f3155e. No implementation, build,
query, profile or VM work. H089 remains complete in its accepted narrow scope;
this does not expand it or rerun the DP control.

## One concrete missed projection

For a materialized entry-table consumer such as
`m.entries().select("quality")`, the current custom entry operator still gathers
ALL row, column and entry fields before the ordinary projection discards them.
Its singleton path also concatenates full input batches first; duplicate-key
runs can spill those full batches. Omitting output takes alone therefore misses
an earlier copy and can still touch large unused nested payload.

Choose **consumer-local output/materialization projection before the logical
extension and consumer schemas freeze**, with a borrowed narrow input view
inside the operator before any concat/spill/take. Execute the original upstream
producer unchanged. Initially retain all row/column keys and only support simple
column selection/alias/reordering. This is a general entries-table projection,
not an aggregate/field-name/benchmark shortcut.

Public users already have a manual workaround: select row/column/entry fields
on the matrix before entries(). The objective is to make a normal entry-table
selection avoid equivalent unused copying automatically, without changing its
producer/effect domain.

## Current source and evidence

- Python matrixtable.py:1602 uses ordinary parallel context/explode when row-key
  uniqueness is certified. Unknown/duplicate keyed rows instead reach
  matrix_entries_by_key_runs at1656. Do NOT claim this affects every entries()
  query. Native Hail-origin tables can have unknown uniqueness; public rekeying
  to duplicate keys is another legitimate path. Future actual-plan confirmation
  must identify which path is reached; no private metadata editing to force it.
- Table.select (__init__.py:659) automatically retains keys, then native.project
  produces MapRows. Matrix entries' _with_globals introduces row-identity
  MapGlobals wrappers. Initial preparation may cross only these row-identity
  wrappers with unchanged keys/schema, retaining their exact globals. Sorting,
  changed keys, filters, joins, opaque/effectful projections remain barriers.
- logical.rs:1113 currently recognizes direct Read projections, but otherwise
  lowers the child before constructing the standard DF projection. The generic
  VailNode captures its schema (337/359, extension at446); frozen OrderedNode
  also retains its captured schema (101). A late physical schema mutation is
  not a valid substitute.
- partitioned.rs:1296 prepares MatrixEntriesByKeyRuns with all row/column field
  names and full output schema. matrix_entries.rs:673/854 searches names and
  takes all fields; select_entry_fields at842 iterates every entry child.
- **Earlier materialization:** singleton concat_batches is at764; KeyRun
  spill_batch at90 writes the retained batch. Narrow BEFORE those boundaries,
  not only inside build_output_batch.
- matrix_entries.rs current SHA256 remains
  b364fe2182714f6a997c0ff2c93646c9ac05fad9080758c1da2c30e623751636,
  identical to the historical native entry-projection investigation. That report
  records residual MutableArrayData/list-take work and an ineffective physical
  EmbeddedProjection trial blocked by ConsumerScope. Its successful key-bound
  lineage fix57935256 is already an ancestor of current main. Do not repeat that
  fix or weaken ConsumerScope. Historical samples include startup/BLAS and are
  not current attribution or a speedup prediction.
- The accepted H089 AD/DP timing packet concerns a different global owning-list
  admission. It provides no current profile evidence for this entries operator.

Reference: ../20261023-native-entry-projection/README.md, especially “Profiles
and remaining work.”

## Narrow design to review before code

1. **Prepare a consumer derivative before freezing.** At MapRows lowering,
   recognize ordinary Column references/aliases over MatrixEntriesByKeyRuns,
   optionally through same-key row-identity MapGlobals wrappers. Derive a
   deduplicated selected-original-field set in original row/column/entry order.
   Require every original output key to remain available under its original
   name. Preserve the ordinary outer DataFusion projection for requested aliases
   and output order. Identity/all-fields selection returns the original route.
   Any unsupported expression/binding/capture/effect/wrapper/key change returns
   the ENTIRE original route; no partially optimized sibling set.

2. **Give the derivative its own valid schema/identity.** An internal projected
   MatrixEntries descriptor/table has the narrowed output schema at creation,
   rather than rewriting an existing frozen node's child schema. Original table,
   source/global plans, predicates, ordering and producer expressions are not
   mutated. Do not cache a consumer-specific derivative under the original
   table-only cache key. Any memo is action-local and keys on original identity
   plus the exact projection descriptor. Keep independent consumer/reset/replay
   contracts intact; no pointer-only global cache.

3. **Bind a private validated output/view descriptor once.** Resolve source field
   indices, selected child Fields (including metadata/nullability/logical type),
   raw input positions, private narrow-frame positions, key aliases, partition
   tag, and output schema during query preparation. Use separate prepared row,
   column and entry index vectors; no field-name/type decisions in entry loops.
   Preparation binds schema/indices/key/offset contracts and provenance. EACH
   batch view then binds its actual child lengths, parent masks, empty-Struct
   length and original offset owners through safe constructors; preparation
   cannot pre-certify unknown runtime buffers. The private descriptor cannot be
   publicly fabricated or carried across a type-changing rewrite.
   All keys and the private tag remain demanded internally. Initial key-retaining
   scope leaves the original composite ordering meaningful; rebind key columns
   against the new schema/DF projection mapping, never claim sorting by a suffix
   after dropping a preceding key. Input distribution, source partition/key-bound
   grouping and spill/run order do not change.

4. **Borrow narrow views BEFORE materialization.** Once an original input
   RecordBatch has arrived and its producer has evaluated, construct one private
   narrow batch view for that batch: selected row Struct children, selected entry
   value Struct children, every required ancestor validity, original validated
   List/LargeList offsets, original key aliases and internal tag. Use safe Arrow
   Struct projection/constructors and cloned validated OffsetBuffers with the
   prepared Fields. No take/concat/ArrayMap/payload copy constructs this view.
   Safe Struct constructors may still inspect logical_nulls and
   parent.contains(child_nulls), scanning large masks: there is no O(fields),
   metadata-only or zero-work claim. Include large nullable-mask controls and
   constructor-callee attribution; no unsafe/build_unchecked shortcut,
   requiredness weakening or independent Arrow fix.
   Preserve empty-Struct length AND parent validity when zero entry payload
   fields are selected. Unsupported declared representations decline admission during preparation
   and retain the old whole route. Do not switch schemas midstream on a bad
   batch or fabricate a contiguous list over disjoint owners.

5. **Feed those views to existing algorithms.** Capture ORIGINAL source_batch_id
   and source_batch_bytes BEFORE constructing any narrow view. Keep grouping and
   comparators and
   original conservative source-batch memory charges; perform the view before
   singleton queue concat and duplicate-run spill. Original producer batch bytes
   may overestimate retained narrow data, which is deliberately safe. Do not
   loosen the64MiB KeyRun/singleton source charge or8192 pending-singleton cap.
   Output sizing is separate: preserve the32MiB/8192-row output policy and
   original per-chunk estimate/limit rather than silently recalculating it from
   smaller views. Any sizing change needs its own explicit review/control. Shared column metadata
   resolves through its existing action cache; form a consumer-local borrowed
   selected-column view afterwards, never mutate the shared batch. Existing
   Arrow slice/take kernels perform actual required expansion, only for selected
   fields. The existing width/presence/error checks stay in scope even if no entry
   payload fields are requested. No new scheduler, sorting, repartitioning,
   recursive compactor or scalar cell interpreter.

### Nulls, owners and resource limits

Entry absence still comes from cell/value-Struct validity, not selected-field
nullness. A defined entry whose selected value is null still emits a row; a
missing entry does not. The borrowed view retains every relevant parent mask and
List offset. It must not turn a missing value Struct into a defined empty Struct.
Ordinary Struct/List validity represents their logical nulls; do not generalize
this to dictionary/union/encoded wrappers without their logical-null contract.

Unselected nested fields must be dropped by view binding **before** any nested
Arrow concat/spill/take can traverse them. A selected field remains subject to
its existing copy behavior; this is not a recursive hidden-payload compaction
fix and must not claim one. Views may retain original buffer owners; whole input
backing/producer IO can remain large even when consumer copied bytes fall. Input
buffer retention, selected output bytes and allocation/peak measurements must be
reported separately. Preserving old memory charges avoids pretending that a
borrowed slice releases an entire input owner.

## Existing DataFusion/Vortex machinery to reuse

Keep standard logical/physical ProjectionExec for aliasing/reordering and
aggregate/join optimization outside the semantic boundary. The pinned DF55
ExecutionPlan::try_swapping_with_projection hook is legitimate, but the historical
trial proved it alone does not cross frozen consumer wrappers. Do not disable
optimizers, bypass consumer identity, or alter the DF projection correctness
patch. Bind a valid projected extension before that boundary instead.

Use existing Arrow Struct projection, safe List/LargeList reconstruction with
validated offsets, existing Arrow slice/take/concat and IPC spill, plus DF
projection/schema property utilities. No handwritten SIMD or new dependency.

Copies in unchanged upstream column realignment (matrix_arrays take) are also
outside this local operator change and need separate profile attribution. No
claim removes all unused-field copies across the whole graph.

Initial source execution/IO remains unchanged. Existing SourceDependencies,
provider_with_nested_projection_paths and native Vortex scan/layout projection
are future reuse points ONLY after a separate producer-demand/effect proof.
This proposal neither adds a Vortex codec/layout nor claims fewer source bytes
or decoded fields. It only stops materializing outputs the consumer discards.

## Public proof and resource gate (future authorization)

- Legitimate duplicate/unknown-key MatrixTable, stored in native Vail format,
  followed by entries().select(arbitrary fields/aliases). Public outputs must
  match keys, values, duplicate row/column multiplicity and observable composite
  order. Compare pure stored-input manual matrix preselection as a reference;
  do not use plan topology as a test oracle.
- Filtered entries versus defined entries with null selected fields; List and
  LargeList slices/nonzero offsets; zero columns/rows and no entry payload;
  nested/call logical metadata, projection aliases/reordering, head/collect,
  grouped consumers and repeated actions. All input-width errors remain.
- Shared-DAG consumers: different projections and full+projected consumers in
  one action and across action resets must not corrupt schema or consume a
  shared partition twice. Globals/RNG/lazy producer errors retain original
  evaluation: tests must consume failing values rather than assert a private
  plan/effect label. Unsupported projections must keep the whole old route.
- Public Arrow/TableIR fixture, constructed before accounting: fixed selected
  results/keys/owner count, growing UNUSED nested payload (including a null
  nested value with a large hidden child span). Force multiple singleton batches
  and duplicate-key runs so concat/spill/take are actually exercised. Only
  selected fields may be copied/serialized; no test-only export/counter. Use
  consumer allocated bytes/public results, not flat total RSS when backing input
  grew. Fixture production is outside allocation/IO accounting. Public spill proof
  uses process/temp-write IO around the real action; both baseline and candidate
  multi-batch duplicate-key runs exceed the UNCHANGED original-source64MiB
  budget. Report allocation, spill IO and whole-process RSS separately. Do not
  monkeypatch the budget, use private spill counters, or confuse threshold
  crossings/fewer triggers with removal of unused bytes.
- Before implementing, confirm the selected current-main public pipeline reaches
  this custom operator and profile the actual unused-copy path on an authorized
  cloud slot. A GQ-only entries projection is a possible existing-fixture probe;
  no DP rerun or H089 expansion is proposed. STOP if it reaches another path,
  input/effect proof fails, or required copying cannot be removed without
  equivalent materialization elsewhere. Historical profiles do not satisfy this
  new runtime gate.
- After source/runtime review: matched independent-process wall/core-second,
  consumer allocation/copied bytes, spill bytes and whole-process memory, with
  full outputs and unchanged full-projection control. No promised multiplier;
  retain adverse cases. No VM or build is authorized by this document.

## Scope and deferred items

Likely surface: private projection preparation at logical MapRows lowering;
projected descriptor/schema on the MatrixEntries semantic node; partitioned
configuration binding; batch view and selected-field emission in matrix_entries;
public tests. No generic graph-wide input pruning or consumer-wrapper rewrite.

H051 receives this one output-projection investigation. Its repeated width tests
and widened offsets are not independently changed here. H028 transpose scratch,
H012 scalar_rows fallback, H008 scalar span choice and H052 descriptor validation
remain separate queued work; normal aligned maps already use evaluate_flat and
prepare_row_captures already occurs outside the row loop. No false claim that
all row-capture pipelines recompile a body per row, and no closure of those IDs.
