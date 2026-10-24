# Native repeated-reduction investigation

Base: `40de9e4e`. This branch investigates reusable native entry computations,
not benchmark-specific patterns. No performance improvement is claimed yet.

## Source findings

Pinned DataFusion 55.0.0 `common_subexpr_eliminate.rs` uses expression identity, rejects
volatile nodes, and tracks conditional evaluation of CASE, short-circuit Boolean
operators, and UDF-declared conditional arguments. We should give this optimizer
sound identities rather than add another CSE pass.

Vail's `logical_kernel` exposes free source columns as ordinary UDF arguments and
includes actual Arrow argument types in the semantic identity. However, the
remaining expression identity includes arbitrary callback parameter names.
`ArrayMap`, `ArrayFilter`, `ArrayPredicate`, `ArrayFold`, `Switch`, and aggregate
binders can therefore distinguish alpha-equivalent computations. This explains
why sibling fractions can retain separate identical presence denominators.

Proposed correction: normalize bound lexical names in the semantic identity,
without modifying executable expressions or free capture names. Reuse
`Expr::child_binders` as the lexical edge contract. Keep types, actual arguments,
effects, RNG scopes and error barriers unchanged. Scope-sensitive memoization
must preserve DAG sharing without confusing nodes reused under distinct binders.

Validation still required: public reused/nested callback queries, distinct free
captures and shadowing, missing/filtered entries, conditional errors and RNG;
normal build and measured multi-reduction pipelines against the exact baseline.
