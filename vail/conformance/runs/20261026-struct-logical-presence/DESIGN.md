# Logical struct presence: separate source-only design

No implementation/build/admission changes. Reader candidate615/f998 remains
frozen; current-main compositionef35 is separate. Actual structural IO receipt
shows49.04%fewer application-reader bytes, not bitmap-only presence.

## Why a value dependency survives

StructReader::expand_struct_root changes a nullable parent field into
Mask(field,parent_validity). IsNotNull(GetItem(selected,Root)) consequently becomes
IsNotNull(Mask(selected,parent_validity)). Existing field partitioning retains
the selected Struct as a VALUE operand alongside the parent mask; that child
reads its four payload fields. The~2MBremaining in the public64×1024fixture
matches that selected struct payload. This is a source-grounded interpretation,
not an encoding/read-range measurement attributing every residual byte.

## Exact logical identity and limits

Mask requires its predicate to be Bool NONNULL. For logical validity:

    present(Mask(v,m)) = present(v) AND m
    missing(Mask(v,m)) = NOT(present(v) AND m)
    GetItem(f,Mask(s,m)) = Mask(GetItem(f,s),m), for Struct s

All resulting predicates are genuine Bool NONNULL. False values and NaNs are
PRESENT; parent/target logical nulls are absent. A nullable mask is not admitted
by the Mask vtable and must never be treated as nonnullable physical bits.

These identities alone do NOT justify a universal scalar optimizer rewrite:
Mask can avoid evaluating a fallible value when its predicate is false, whereas
an eager rewritten Boolean expression could evaluate it first. Unknown functions,
casts/parsers, bindings and effectful expressions must preserve their existing
error/evaluation contract. Do not introduce a global rule just because the
value-level truth table agrees.

## Narrow general reader preparation

The first safe implementation should recognize only pure, typed Root-relative
GetItem chains under IsNotNull/IsNull while preparing a STRUCT layout reader.
Before ordinary root expansion inserts generated parent masks, express that
presence demand as independent parent validity plus the same presence demand on
the first field's child reader. That child owns its own logical ancestor mask.
Each nested StructReader repeats this at its boundary. Ordinary value consumers,
arbitrary scalar masks/functions and mixed raw siblings keep their original
expanded expression. No terminal count operator or matrix/field-name matcher.

Equivalent partition expression for path first/tail:

    parent_validity AND IsNotNull(GetItem(tail, GetItem(first, partition_root)))

Here the inner first-field reference has the actual child schema, without the
outer parent's generated Mask. The outer parent validity remains explicit and
is applied exactly once to the NONNULL Boolean result. IsNull uses complement.
Required parents contribute true; handling constant-only partitions must use the
existing public row-count/mask machinery rather than synthesize payloads.

Perform schema/path decisions once in prepared reader state. Reuse existing
bound builtins, typed child projection, partition cache and bitmap operators.
Memoize summary/rewrite by retained ExactBoundExpr identity, preserve DAG sharing,
and record unavoidable remaining schema hashing. Do not claim universal linear
planning/execution: existing nested reader preparation has other traversals.
No per-row/entry callbacks, scalar interpreter loops, fake structs or SIMD kernel.

## Public proof before resource claims

Use public writer with explicit structural-list edition and actual compact
layout tree. Retain the existing independent modulo5/7 ancestor oracle. Add
several nested field levels with required and nullable ancestors; defined empty
structs; false Bool and NaN leaves; null leaf vs null parent; non-byte-aligned
slices; missing/empty lists; same predicate sharing, raw/different-field siblings,
and bounded range/sparse mask. Test both IsNull and IsNotNull exact dtypes/results.
Value consumers must retain their original schema and values.

Add a fallible scalar/cast sibling control to show the narrow matcher declines
arbitrary expressions; do not claim a global change to lazy/eager evaluation.
Use unchanged data fields and real FileReadAt traffic, comparing current reader
against the candidate, with open/prefetch overlap and unique/requested bytes
reported separately. Fresh/reused tiny/many files and raw/ListLength/SumMap
controls guard generic reader preparation/caching costs. No private plan-name or
identity assertions and no latency/byte-threshold unit-test oracle.

## No automatic storage admission

A Struct layout can separately address validity; a Flat target may still read
payload buffers. Codecs, descendant logical masks, selected ranges and full
immutable effective domain need a DISTINCT capability proof before native
computed admission. Sparse/missing-outer/orphan/dictionary/unknown-codec shapes
remain ordinary fallback unless explicitly proved. Post-canonical correctness or
mask algebra does not certify bounded pre-decode work or selective IO.
