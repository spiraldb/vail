# Vail total key ordering

`vail-ordering` defines one canonical byte encoding. DataFusion sorts these bytes,
groups and joins on them, and the Iceberg fragment index compares the same bytes.
Reads concatenate certified Vortex fragments in index order without sorting the
table. New local tables use `vail-total-order-key-v3`. Keyed snapshots using the
retired `vail-hail-key-v2` comparator are rejected with a rewrite-required error;
their fragment bounds cannot certify the new order. Primitive-only
`vail-primitive-key-v1` snapshots retain their reader.

## Order by type

| Type | Ascending order |
| --- | --- |
| Missing | After every present value, at every nesting level |
| Boolean | `false < true` |
| Integer | Numeric order |
| Float32 / float64 | IEEE 754 `totalOrder` at the declared width |
| String | Lexicographic Unicode scalar order, encoded as unsigned UTF-8 bytes |
| Array | Lexicographic element order; a strict prefix precedes its extension |
| Tuple / struct | Lexicographic element / declared field order |
| Set | Lexicographic comparison of sorted unique members |
| Dictionary | Lexicographic comparison of entries sorted by key, comparing key then value |
| Interval | Start point, start inclusivity (inclusive first), end point, end inclusivity (exclusive first) |
| Ndarray | Shape first, then flattened row-major elements, both lexicographically |
| Call | Existing Hail packed signed 32-bit order |

Call and ndarray do not have an unambiguous natural total order. Call retains
its existing rule; ndarray uses the shape-first convention above. These choices
were explicitly reported for user review. Locus remains deferred with reference
genome support; it cannot receive an ordering certificate.

Float encoding preserves all bits, including NaN sign, signaling/quiet bit and
payload. Negative NaNs precede negative infinity; positive NaNs follow positive
infinity. `-0.0 < +0.0`. Negative NaN payload order is the reverse of positive NaN
payload order. Equal key bytes mean the same floating-point bit pattern.
The implementation is verified against Rust's
[`f32::total_cmp`](https://doc.rust-lang.org/std/primitive.f32.html#method.total_cmp)
and [`f64::total_cmp`](https://doc.rust-lang.org/std/primitive.f64.html#method.total_cmp).

At a DataFusion `AggregateExec`, grouping expressions return canonical binary
keys. DataFusion's `any_value` preserves representative original values for the
output. Nested-scope joins compute the same canonical key on both sides. Set
literal ingestion sorts and deduplicates using this same encoder rather than a
separate float comparator. Storage uses saved logical types because Iceberg can
rename nested Arrow fields; fragment certificates bind those types to the schema,
comparator version and snapshot.

Expression comparisons also lower to DataFusion comparisons of canonical keys.
Outer missing operands remain null, retaining three-valued expression semantics;
nested missing values use missing-last ordering. Interval predicates use this
null-preserving encoding with DataFusion comparisons and `CASE`; see
[INTERVALS.md](INTERVALS.md).

Python/native JSON transport carries NaN bits instead of a generic NaN tag, so
NaN payloads and signs survive ingestion and collection. Python's own sets and
dictionaries still use Python equality: they merge signed zeros before Vail can
ingest them, and ordinary Python set localization cannot represent both signed
zeros as separate members. Typed list input for a Vail set preserves them in
native execution; a custom localization representation is a separate issue.
Python/CPU conversions between float widths can also quiet signaling NaNs;
native Arrow values and typed wire bits retain the declared-width representation.

Tests cover both float widths, NaN signs/payloads/signaling bits, signed zeros,
infinities/subnormals, missing values, nested types, grouping with scoped joins,
distinct, expression predicates, write/read, and two-file fragment selection.

Validation on 2026-09-29: 71 tests passed across the ordering, execution, storage
and Table crates, plus two explicit Python-extension Rust unit tests. The
Python/conformance suite passed 337 tests and 19 subtests with four known
unfinished interval-read binding tests deselected. Those four fail because
`NativeTable.filter_intervals_json` is not implemented, independently of ordering.
