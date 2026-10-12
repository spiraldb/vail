# Vortex comparison audit

Audited on 2 October 2026 against **Vortex 0.86.1**, the version pinned in Vail's Cargo.lock, and the checked-in Iceberg Rust integration. This is an audit and implementation recommendation; no query or ordering semantics were changed.

## Results

| Values | Vail versus Vortex | Evidence and consequence |
|---|---|---|
| int32/int64, booleans | Agree | All six comparison operators tested, including integer extremes. |
| Strings | Agree | UTF-8 byte order; tested empty strings, embedded NUL, accents, and supplementary Unicode. |
| float32/float64 | Agree | Both implement IEEE totalOrder and bitwise equality. Negative NaNs precede negative infinity; positive NaNs follow positive infinity. Signed zeros, signaling/quiet NaNs, and NaN payloads remain distinct. Tested all six operators over 256 pairs for each width. |
| Float constant folding and compressed execution | Agree in tested cases | Tested f64 array/constant, constant/array, constant/constant, and BtrBlocks-compressed/constant comparisons across NaNs, zeros, and infinities. This is not exhaustive coverage of every encoding or layout. |
| Missing outer operands | Agree for expressions | Both return a missing boolean if either operand is missing, including two missing operands. Key equality/grouping and sort null placement are separate contracts. |
| Present structs/tuples and arrays | Agree where nested fields/elements are present | Lexicographic field/element comparison followed by list length. Struct tests cover the tuple physical representation. |
| Missing nested values | **Differ** | Vortex orders a missing struct field or array element **first**; Vail orders it **last**. Two nested missing values compare equal in both. A present struct/array does not become a missing comparison result merely because a child is missing. |
| Sets | **Raw representation differs** | Vail ignores physical member order and duplicates; Vortex sees a list and compares its physical sequence. `[1,2]` versus `[2,1]` is equal as a Vail set but unequal as a Vortex list. |
| Dictionaries | **Raw representation differs** | Vail canonicalizes entries for comparison; Vortex list/map comparisons are entry-sequence based. Reversing entries changes the raw Vortex comparison but not Vail dictionary equality. Vail currently stores dictionaries as lists of structs, not Vortex Map values. |
| Intervals | **Endpoint layout now agrees** | Stored as start, start_exclusive, end, end_inclusive. Ordinary struct field/boolean order matches Hail endpoint order. Public includes_start is the negation of stored start_exclusive. General nested missing-value differences remain as documented above. |
| Calls | **Raw representation differs** | Vail uses Hail's packed call order; Vortex compares allele list then phased flag. Tested `[0,2]` versus `[1,1]`: opposite ordering. |
| ndarrays | **Raw representation differs** | Vail orders shape before data; current physical struct stores data before shape, so Vortex orders data first. Tested a length-two zero vector versus a length-one vector containing one: opposite ordering. |
| Valid loci of the same reference | Agree by representation analysis | Stored contig ordinal, contig name, then position align with Vail's locus encoding for validated values. Vortex itself does not enforce reference identity or coordinate validity; Vail must retain those checks. This row was inspected, not exercised by the new audit test. |

Other Vortex types (unsigned integers, decimals, binary, temporal extensions, unions, and variants) do not establish parity for additional Vail APIs. Vail does not expose all of these logical types. Comparisons across widths require explicit casts to the same promoted type; Vortex's binary comparison is not a substitute for Hail/Vail promotion rules.

Vortex's primitive comparison comments say “NaN is the largest value,” but the implementation calls Rust `total_cmp`: **negative NaNs are below negative infinity**. The tests verify implementation behavior rather than relying on that abbreviated comment.

## Separate Iceberg pruning mismatch

A real Vortex file containing a positive NaN was written through Vail's Iceberg writer. The Iceberg scan with predicate `x > +infinity` produced **zero file scan tasks**, while the direct Vortex comparison of the stored value returned **true**.

The checked-in inclusive metrics evaluator assumes an all-NaN file cannot match an inequality. Its min/max logic also needs review for mixed finite/NaN files because NaNs are excluded from the writer's bounds. Thus, matching row-level float comparison semantics does **not** make the complete Iceberg predicate pushdown path safe yet.

An inexact-pushdown residual filter cannot repair this: once a matching file is discarded, its rows never reach the filter. Either repair Iceberg's float metrics pruning for total-order predicates, or use a conservative metadata predicate (including possible NaNs) while retaining the exact Vortex row predicate. Until that is done, do not push total-order float predicates unchanged through that metrics path. `is_nan` in the Vortex reader already handles both signs using comparisons beyond both infinities.

## Recommended semantics and representation

- Keep IEEE float semantics: Vortex already matches the user's requested behavior.
- **Decision: adopt Vortex's missing-first order for nested values**, accepting the Hail divergence to use library kernels without a null-order compatibility adapter. Implementation is pending. Apply it consistently across expression comparisons, keys, joins, grouping, sorting, and fragment bounds. This intentionally changes the existing Hail-compatible missing-last policy and requires a new comparator version plus rewritten/rebuilt bounds; do not reinterpret existing certified indexes with a different comparator.
- Keep value-based set/dictionary semantics. Instead of making equality depend on insertion order, canonicalize their stored member/entry sequences so library list comparison can implement logical comparison. This must also handle null ordering, duplicate members/keys, and nested collections consistently.
- Decide separately whether to adopt Vortex's physical order for calls/ndarrays, or change their physical representation to match the desired logical order. These are Vail logical wrappers, not Vortex-native Interval/Call/NDArray comparisons. Blindly delegating their current structs would change meaning.
- Retain reference-genome compatibility checks for loci.

## Provider integration work

The current TableIR lowering constructs DataFusion physical expressions and operators directly. There is actual integration work: supported filters/projections must become logical DataFusion expressions and reach a TableProvider scan. We do not need to rewrite every TableIR operator before getting pushdown: a read/filter/projection logical-plan island can use DataFusion optimization and physical planning, with existing physical operators consuming its output. Expanding the island through safe transformations gives optimizer-driven pushdown further up the tree. Passing extracted filters explicitly to `scan` is also possible, but then Vail owns that pushdown analysis.

For the ordered source, prefer a small Vail TableProvider adapter, not a copied fork of the whole Iceberg provider. Reuse the public `iceberg_datafusion::physical_plan::convert_filters_to_predicate`, Iceberg `plan_files`, and Iceberg's Vortex reader. Add snapshot-pinned certified fragment selection, independent partition plans, and accurate ordering properties. The stock `IcebergTableScan` is a single unordered partition and cannot simply be delegated to unchanged. Do not expose a schema or ordering claim that loses Vail's logical type and comparator contracts.

The comparison audit does not implement this provider or change Vail's execution path.

## Reproducible evidence

Run:

```sh
cargo test -p vail-storage --test vortex_comparison_audit
```

**11 audit tests pass.** Several intentionally assert observed differences; a passing audit is not a claim of universal parity or corrected pruning. See [test source](../crates/vail-storage/tests/vortex_comparison_audit.rs) and [saved run](../conformance/runs/20261002-vortex-comparison-audit/summary.json).

Source anchors:

- Vortex 0.86.1: `vortex-array/src/dtype/ptype.rs`, `scalar_fn/fns/binary/compare/{mod,primitive,nested}.rs` and comparison tests.
- Vail: [canonical ordering](../crates/vail-ordering/src/lib.rs), [comparison lowering](../crates/vail-execution/src/expressions.rs), and [physical scan lowering](../crates/vail-table/src/plan.rs).
- Iceberg: [provider](../patches/iceberg-rust/crates/integrations/datafusion/src/table/mod.rs), [filter conversion](../patches/iceberg-rust/crates/integrations/datafusion/src/physical_plan/expr_to_predicate.rs), [Vortex predicate reader](../patches/iceberg-rust/crates/iceberg/src/arrow/reader/vortex.rs), and [inclusive metrics evaluator](../patches/iceberg-rust/crates/iceberg/src/expr/visitors/inclusive_metrics_evaluator.rs).

## Interval layout update — 2026-10-02

Intervals now store `start, start_exclusive, end, end_inclusive`. Construction and accessors convert the start flag; public `includes_start`, `includes_end`, start/end, containment, overlap, expansion and string methods retain their behavior. Canonical ordering bytes retain their existing endpoint semantics. The physical schema changed: tables written with the former interval layout must be rewritten; no automatic old-layout migration is included. Differential tests exercise all six Vortex comparisons across endpoint points/flags and compare against the Hail endpoint tuple order.
