# Shared named annotations and demanded list results

Candidate production b81205d0, based on main3dfd7318. Normal Rust1.98 release/kache build in isolated worktree target; no RUSTC_WRAPPER changes or compiled-library copying. Native SHA2568e85c492309e290f5cb273a553d6b6a8be0bde3af210e17709ba3d822d1461a5. Editable Python comes from this worktree, not an overlay of a different native artifact.

## API scope audit

See api-audit.txt for confirmed boundaries and unaffected APIs. Compatible named Matrix row/column annotations now preserve a shared aggregate struct before lowering and unpack the original output names afterward. Nonaggregate/key fields, unsupported reducers, RNG, lookups, native initializers and position-sensitive callbacks preserve their existing routes. Plain row fraction keeps its old route because its existing empty identity differs from generic composite reduction. Table projection/grouped aggregate APIs already pass their complete field scope to native code; no changes made there.

Lookup preparation now returns original expressions when there are no lookups. Real lookup replacement, matrix field rebinding and list-reducer rewrites preserve node identities within their lexical scope. This alone preserved four unique info-score reducers but did NOT eliminate repeated runtime computation: first diagnostic100.95CPU vs106.33 baseline; do not call that a completed speedup.

The subsequent demand-preserving binding pass creates lexical vector bindings only where a repeated expensive node is already unconditionally demanded. IfElse contributes only its condition; short-circuit AND/OR only their first operand; Switch and callback bodies remain opaque; RNG expressions are excluded. Shared n, theta and probability-sum expressions in info-score are then evaluated once in their owning selected scopes. Uses existing native cheap lexical binding, not generated kernels or increased parallelism.

## Source fixes during review

Initial binding revision5ce42684 had two review findings: repeated descendant analyses could be quadratic along long chains, and detached Switch case bodies could lose their lexical binder. Correctedb81205d0 retains the DAG/views once, computes postorder bit summaries once and keeps Switch bodies opaque. Complexity depends on unique edges, shared-candidate bitset width and distinct binding environments; no claim of strict linearity independent of candidate count.

Cached NativeExpr views are also essential for actual substitution: inline Vec child inspection creates new wrapper identities on a fresh view. v1 could therefore add unused binding arguments without replacing all old body references. v2 replacement uses the retained views and produces four actual lowered list-reducer occurrences in the info-score diagnostic. Repeated-name counts in physical Display alone are not execution counts.

## Validation so far

38 public focused tests pass (focused-tests.log), including independent sparse/empty/global/unsupported reducer results, annotate/select/transmute output schemas, shared genomic summaries, unselected failing reducer branches, Switch lexical scope and an80-level arithmetic chain. Earlier39-test log predates two new tests and helper rework; it is retained as historical evidence, not final broader validation. Some intermediate temporary bug runs failed and were corrected before these logs; no failures are waived.

Both frozenbaseline and candidate already execute errors in projected-away bad aggregate siblings at Matrix row/column annotation boundaries. This pre-existing behavior is not claimed fixed or introduced here. Unselected conditional failing branches remain lazy and are explicitly tested.

Final exact same-artifact cost comparisons and broader tests are pending. Source review is clear atb81205d0; final integration approval remains pending matching evidence. No main changes by this agent.
