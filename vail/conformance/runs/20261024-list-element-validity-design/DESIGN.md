# Typed list-element validity projection: design checkpoint

SOURCE ONLY. Base: accepted grouped API9ef08e91 + evidence66d92034. No library
implementation, planner eligibility expansion or new build. Public grouped
correctness/resource proofs exist; native count-only read pushdown does not.

## Missing operation and first public contract

StructReader already rewrites IsNotNull(field) into ancestor-aware logical
validity. ListReader::fetch_elements_for_projection only sends ProjectPaths to
its element reader; no value paths means raw elements. The missing operation is
list_element_project(input, body), returning a GENUINE new list element type.
First prepared body: IsNotNull(GetItem(...Root)), yielding list<non-null Bool>
with original declared outer-list nullability. False is a present Boolean value,
not a fabricated null struct under the old schema. No matrix/DP/fixture matcher.

Bind body, resolve paths, select operation and compute exact input/result dtypes
once. Admit only this closed pure body initially: arbitrary foreign scalar
functions cannot be assumed pure. Reuse expression protobuf serialization and
session registration. BoundExpression::with_children recomputes node dtypes;
do not copy old cached dtypes after changing element schema. No per-entry Rust
callback, schema walk or manual Boolean loop.

## Reader wiring and residual: apply body exactly once

Prepare (element_body, original_element_dtype, new_list_dtype, residual_expr).
Pass element_body into the EXISTING element LayoutReader::projection_evaluation;
ListReader owns offsets/outer validity and constructs the transformed typed list.
For exact Map(Root,body), residual is Root(new_list_type). For Sum(Map(Root,body)),
residual is Sum(Root(new_list_type)). Rebuild outer nodes with with_children and
verify promised final dtype. Evaluating the original Map again on Boolean
children is incorrect.

First scope accepts one compatible transformed root per projection tree.
Repeated identical prepared bodies may share it. A raw Root sibling, different
body or consumer requiring original values declines the computed route and uses
original source/expressions. Later real struct-valued multi-projection bundles
can support independent maps; no placeholder schema or accidental lambda capture.

Both current full/bounded ListReader branches fetch elements concurrently with
outer validity. New native admission must be proved BEFORE entering that route.
Do not silently treat bounded min-offset..max-end envelopes as sparse proofs.
Existing SUM/ListLength eligibility is unchanged.

## Closed native proof, distinct from the public library operation

Add a typed operand identity containing list path, target path, body kind and
exact input/output schema. Cache/certificate keys include the operation, not
just paths, and bind immutable file/snapshot/task/projection identity as existing
PreparedComputedProjection does. A DenseI32Sum certificate is not automatic
permission for arbitrary validity expressions.

First admission requires:

1. Full EFFECTIVE row coverage: no query mask, delete, clipped row range, sparse
   task subset or later pruning; exact task domains have no holes/overlap.
2. Checked dense offsets start0/end at actual child cardinality; checked rows*
   width/count arithmetic. Every effective outer list is PROVEN logically valid,
   even if declared nullable. Orphan children under missing outer groups are
   excluded from native eligibility despite public grouped output correctness.
3. Nested struct layouts/field identity match the input schema, with exact
   element-domain lengths for ALL ancestor/target validity descendants. No mask
   union over a larger retained physical owner before clipping.
4. Closed decoded work/temp bounds for each selected Boolean validity encoding:
   constants, canonical bitmaps and individually audited bounded compositions.
   Unknown codec/composition/dictionary uses original typed DF before decode.
   Reuse validated serialization, metadata budgets and file-handle/read reuse;
   compressed bytes are not decoded allocation bounds.
5. Dictionary nullness includes codes AND dictionary-value validity. Their
   recursive domains/scratch must be bounded against corresponding selected
   codes; a generic whole-owner take fallback is not an admissible proof.

Selection changes invalidate proof before payload reads or fall back wholesale.
Public all-missing Pco timing AFTER canonical acquisition proves none of these
pre-canonical native guarantees. Supporting missing outer lists/slices/sparse
queries later needs exact child-range/codec relationships, not relaxed guards.

## IO skipping versus decoding skipping

A STRUCT layout validity child is separately addressable: IsNotNull(structpath)
can omit unrelated field segments. A FLAT segment may package numeric payload
and validity buffers together. IsNotNull there can avoid numeric decoding while
still reading payload bytes. Initial IO-avoidance scope should therefore target
real struct-layout presence, with actual child reads/profiles proving omission.
Primitive-field presence is a later extension with separate IO/decode accounting.
Pco exposes validity slots, but array construction/flat-segment reads can still
touch payload. Encoding ID alone is not permission to ignore those costs.

## DataFusion and reduction semantics

Keep relational operators, ordinary projection/filter optimization and final
SUM in DataFusion. A generic row-list predicate/count consumer requests a typed
Boolean list. Count TRUE uses Bool SumV2, not Count(Bool), which counts false too.
Present EMPTY/all-null BoolSum is null and needs explicit zero identity for a
count-like consumer. Missing outer groups keep original consumer semantics and
nullable schema; output nullability follows declaration even on all-valid data.

NaN is defined: do not inherit Count's default skip_nans behavior. Initial
struct-presence avoids numeric values. U64 true-count to signed Vail/DF count
needs checked cast or proof of i64 bounds using effective cardinalities; no
widened per-entry array or changed overflow contract. Unknown bounds fall back.
Original globals/scopes/effects/aliases/cardinality/ordering/TaskContext caps
remain intact. No partial encoded output before fallback and no new terminal
entry-count operator. A generic list projection serves non-count consumers too.

## Public gates before acceptance

- Exact output list/element/consumer dtypes, logical ancestor/leaf nulls,
  present false vs true, empty lists, declared-nullable/all-valid inputs.
- Body evaluated once: direct Map, Sum(Map), same-body repeats; raw-value and
  distinct-map siblings fall back with original schemas/scopes/effects.
- Library missing-outer semantics use proven grouped API; native missing/orphan
  domain still declines. No post-mask repair of an invalid kernel.
- Public native writer/reader fixture: optional struct records with large unused
  payload; actual requested bytes, wall/core-seconds/RSS and samples show fields
  omitted. Count result compared with independent presence oracle.
- Sparse/sliced/hidden owners, dictionary null dependencies, unknown codecs,
  schema evolution/field renaming and later selection changes safely fall back.
- Fresh/reused tiny/many-file preparation controls. Budget remains a cumulative
  charged metadata estimate, not a strict parsed-heap/allocator bound.

No implementation is authorized by this document; request design review first.
