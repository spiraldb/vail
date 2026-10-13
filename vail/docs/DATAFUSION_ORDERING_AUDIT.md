# DataFusion representation and ordering audit

Audited 13 October 2026 against the pinned DataFusion 55.0.0 and Arrow 59.2.0, using direct DataFusion SQL and its native operators. The probe bypasses Vail's custom canonical-key expressions. No production representation or semantic policy is changed by this audit.

**Policy update:** The subsequent decision is to use native DataFusion operators directly for floating-point values. The float-view proposal below is historical and must not be implemented. Set/Dictionary ordering and keying are explicitly unsupported; ndarray work is deferred. See [intentional differences](INTENTIONAL_HAIL_DIVERGENCES.md).

The original audit target was: IEEE 754 totalOrder, including distinct signed zeros and NaN bit patterns, and the agreed nested missing-first order. This is deliberately different from Hail. Vail's current custom comparator still implements missing-last, so the null-order decision remains an implementation gap. Cross-type comparisons must still obey Vail's type promotion and reference-genome checks.

## Type-by-type answer

| Vail type | Does the raw representation have the intended native ordering? | Smallest change or requirement |
|---|---|---|
| bool | Yes | None. |
| int32 | Yes | None. |
| int64 | Yes | None; integer comparison retains precision beyond 2^53. |
| string | Yes | None; unsigned UTF-8 lexicographic order matches Unicode scalar order. |
| float32 / float64 | **No across DataFusion operators.** Native sort distinguishes signed zeros, but ordinary comparisons, grouping and joins merge them. Tested NaN sign/payload/signaling ordering agrees. | Keep stored Float32/Float64. Use a zero-copy single-field struct **key/comparison view** to select DataFusion's nested comparison path. This preserves totalOrder in comparisons, grouping and joins. Copy the original outer validity into the view, so a missing float stays missing. Arithmetic continues to consume the original float column. Direct Arrow float comparison kernels are another efficient option for expression comparisons, but alone do not fix grouping/join keys. |
| array<T> | Yes, lexicographic elements then length, **provided T's representation is correct** | No array layout change. Recursively correct Call/ndarray/unordered-collection elements. Floats already get totalOrder when nested inside an array; the root-level SQL float normalization is not applied recursively. Native comparisons put missing elements first. |
| struct | Yes, declared field order, subject to recursive field representations | No ordinary struct layout change. Native comparisons put missing fields first. A separate zero-field struct emission bug is noted below. |
| tuple | Yes, positional struct fields, subject to recursive element representations | None. Vail's empty tuple already has a constant bool tag; native grouping works for it. |
| interval<T> | Yes, subject to T | Already stored as start, start_exclusive, end, end_inclusive. False start_exclusive sorts first (inclusive start); false end_inclusive sorts first (exclusive end). No further field reorder is needed. |
| locus<reference> | Yes for validated values within one reference | Already stored as contig ordinal, contig name, position. The name is determined by ordinal for valid values and cannot change the order. Retain explicit reference equality and coordinate validation: DataFusion itself does not enforce Vail's reference type parameter. |
| Call | **No** | Replace allele-list/phased struct with the existing Hail packed signed Int32 value, carrying the logical Call tag in metadata/extension typing. Preserve genotype numbering plus ploidy and phasing bits; a bare triangular genotype index is insufficient. Reimplement accessors against this storage using library arithmetic. |
| ndarray<T> | **No** | Store shape before row-major data. Native struct/list ordering then gives the chosen shape-first order, subject to element representation. Update positional accessors and schema/localization metadata together. |
| set<T> | **No as an arbitrary list.** Already-canonical literals are a subset that agrees. | Enforce sorted, unique physical members at every construction/update boundary. A canonical List works; a canonical Map<wrapped-key, Null> also works. Missing members need a present struct key containing a nullable member, because Arrow Map forbids nullable keys. The sorted-keys flag does not sort or deduplicate data. |
| dict<K,V> | **No as an arbitrary entry list** | Enforce unique keys and entries sorted by key, resolving duplicates according to the existing logical rule before comparing. The current List<Struct<key,value>> can then compare natively; changing it to Map is optional. If using Map, wrap nullable keys as above. Both key and value representations must recursively agree. |

These recursive qualifications matter: array<Call> cannot acquire the correct Call order merely because list comparison is lexicographic. Conversely array<float64> already distinguishes signed zeros, even though bare float64 SQL comparisons do not.

## Directly verified counterexamples and candidate fixes

- Bare -0.0 versus +0.0: native SQL equality true, one group, four matches in a two-row self-join. Native ORDER BY of the reversed inputs returns negative zero first, demonstrating the inconsistency between sorting and equality.
- One-field struct views of the same float values: equality false, two groups, two self-join matches, negative zero first. Verified both widths, plus propagation of an outer missing float through a validity-preserving view.
- Call [0,2] versus [1,1]: allele-list storage puts the first call before the second; packed Int32 correctly reverses that pair.
- Ndarray [0,0] with shape [2] versus [1] with shape [1]: current data-first storage orders the first before the second; shape-first storage correctly reverses the pair.
- Set [1,2] versus [2,1] and dictionaries with reversed entry sequences: raw representations compare unequal and produce two groups. Sorted canonical sequences compare equal and produce one group.
- Canonical Map<Struct<nullable member>, Null> supports a missing set member and agrees in comparisons, sorting, grouping and joins.
- Empty tuple's constant-field representation compares equal and groups correctly. Raw zero-field struct grouping raises a DataFusion emission error while decoding its row representation. A constant bool field gives the same singleton value domain and avoids the error; preserve the logical empty-struct type with versioned Arrow metadata.

## Important implementation details

DataFusion's `apply_cmp` calls `normalize_float_zero` for non-nested operands. Its grouping row implementation also normalizes top-level float columns, and its join code applies SQL signed-zero equality. Arrow's primitive comparison kernels and RowConverter already implement totalOrder. DataFusion's nested comparator uses Arrow `make_comparator` without recursive float normalization. The struct view exploits this existing library behavior; no custom scalar comparator is necessary.

Float wrapping can be an internal projection rather than a storage-format change. A wrapper whose parent is always present would turn a missing float into a present struct with a missing field and change outer-missing expression semantics; retaining outer validity is essential.

Native nested comparisons use missing-first by default. Arrow sort options propagate into nested comparisons. If an operation needs outer missing-last but nested missing-first, express that explicitly with separate presence/payload sort fields rather than assuming one SortOptions flag controls the two independently. Outer missing expression operands still yield a missing boolean. Missing ordinary join-key components still must not match.

Canonical collection storage must be maintained by all writers and transformations, including nesting, union, updates and Arrow imports. An extension name or Arrow Map sorted flag is not itself a normalization implementation. Existing native array distinct helpers must also be checked before using them for float members/keys, because SQL signed-zero normalization is a separate concern.

Changing schemas for Calls, ndarrays, empty structs or canonical collection contracts requires consistent Arrow metadata, ingestion, expression accessors, export and storage updates. Implementing the decided null-order change requires a new comparator/version and regenerated fragment bounds; existing missing-last bounds cannot silently certify missing-first scans. Existing Iceberg float-metrics pruning issues are also independent of row-level comparison correctness.

The custom encoder is therefore **not intrinsically required for these logical types**. Library comparison and row-conversion machinery can replace it once representation invariants and the remaining library edge cases are addressed. Persisted bound encoding still needs an explicit versioned contract; using a library does not remove that storage responsibility.

## Reproduction and limits

Run `cargo run --release --offline -p vail-table --example datafusion_order_audit`. The example asserts all six comparisons, group counts, ORDER BY result and self-join counts on representative cases. The zero-field struct case intentionally records the observed library emission failure. Saved observations are in [the audit run](../conformance/runs/20261013-datafusion-order-audit/observations.txt).

This is representative execution plus source inspection, not exhaustive testing of every nested type combination, optimizer rewrite, malformed value, reference or encoding. The candidate representations are demonstrated, not deployed into Vail. A future implementation should retain these checks and expand roundtrip/constant-folding coverage.

Source anchors: `Type::storage_type` and `Type::to_arrow` in vail-execution/src/expressions.rs; DataFusion physical-expr-common/src/datum.rs; common/src/utils/mod.rs (normalize_float_zero); physical-plan/src/aggregates/group_values/row.rs and joins/utils.rs; Arrow ord/src/ord.rs and row/src/lib.rs.
