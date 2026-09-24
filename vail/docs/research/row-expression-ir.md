# Row expressions and DataFusion projection

`annotate`, `select` and `transmute` share a typed native projection path. Python constructs expression objects and checks Table source identity; it does not evaluate rows. A serializable Rust expression IR validates types and lowers to DataFusion expressions. Native Rust callers can construct the same IR without Python.

## Boundary

The Python/native projection payload is an ordered array of `[output_name, expression]` pairs. Names are literal field names, including spaces and punctuation, rather than SQL text. The native bridge exposes `project_json` and `schema_json`; the latter returns the validated result schema. Projections operate on Arrow batches and retain their row count, row sequence and logical partition boundaries, including zero-column results.

Types are primitive strings (`int32`, `int64`, `float32`, `float64`, `bool`, `str`), `{"struct": [[name, type], ...]}` or `{"array": type}`. Nested struct values are JSON objects and arrays are JSON arrays. Special floating values use `{"$float": "nan"}`, `"inf"` or `"-inf"` in a known float-typed position. Decoding is type-directed so a user struct field named `$float` remains a struct field.

The expression wire nodes include column references, typed literals, unary/binary operators, casts, struct construction/field access, conditionals and missingness tests. Their types are checked natively before a projection enters the operation queue. The frontend tracks the input Table and referenced top-level fields, which are needed for source validation and transmute's removal rule.

## Hail projection rules

- `annotate` preserves existing field positions, replaces non-key fields in place, and appends new fields in argument order. Expressions all refer to the input Table, not earlier assignments in the same call.
- `select` emits key fields first, then requested positional fields and named expressions in order. With no key and no requested fields it produces zero-column rows, not an empty Table.
- `transmute` computes its named expressions and removes referenced top-level non-key input fields. It retains keys and all newly assigned output names. Referencing `t.g.y` consumes the top-level `g` field.
- These operations reject key mutation and foreign/stale row sources according to the Hail frontend contract. They preserve existing row order and introduce no sorting.

Schema changes must be tracked at each queue step. An earlier order operation uses its own input schema even if a later projection removes or renames fields. The native bridge therefore stores the source schema separately from the final schema and advances the schema alongside each projection.

## Semantic lowering requirements

DataFusion provides the expression execution machinery. Where Hail differs, lowering must preserve Hail's result types and behavior: wrapping integer arithmetic, floor division/modulo, typed missing values, scalar floating-point rules, and missing conditional predicates. In particular, a missing `if_else` condition produces missing; it must not become SQL CASE's ordinary else branch. Nested field access must include the parent struct's validity.

This IR is also the dependency for later filtering, computed keys, globals, aggregation and scans. Those operations need their own scope and ordering rules. Adding an expression node does not implicitly implement any of them.

Storage uses the new Vortex/Iceberg format through Iceberg Rust PR #2835. Legacy `.ht` decoding and migration remain deferred and are independent of the row-expression implementation.

## Filter and collection additions

`Table.filter(expr, keep=True)` compiles a Boolean expression once per evaluation and filters Arrow record batches in their existing partition order. Missing predicates discard rows for both keep modes. Filtering invalidates exact count metadata; subsequent head/tail operations cannot turn an upper bound into an exact count.

Array construction uses DataFusion `make_array`, and length uses its array-length UDF with the Hail result type. Typed adapters implement Hail bounds errors, strided slicing and numeric reductions. Reductions preserve the element type, integer wrapping, typed zero/one identities, missing-value policy, NaN propagation and signed-zero min/max behavior. These are native expressions, not Python row evaluation. Higher-order lambdas, sets, tuple expressions and scalar variadic min/max remain unsupported.

The production Hail slice implementation has a non-Python edge case for reverse stop `-length`; the source-derived regression records that behavior explicitly. Live-oracle confirmation remains unavailable. See the [review record](../LUNA_IMPLEMENTATION_REVIEW.md) and [verified suite results](../../conformance/runs/20260924T205540Z-integrated-verified/summary.json).
