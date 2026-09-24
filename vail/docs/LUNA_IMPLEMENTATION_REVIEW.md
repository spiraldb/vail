# Luna implementation review

This wave uses one Luna test recorder and two Luna implementation agents. Production edits began only after the recorder reported baseline completion. No upstream assertions were weakened to accommodate Vail.

## Review requirements

Python constructs expressions; Rust executes them through DataFusion expressions and Arrow arrays. Native validation must protect direct Rust/bridge callers as well as the Python frontend. Reuse existing kernels when their semantics match Hail. A semantic adapter must explain the specific mismatch it addresses.

Tests must distinguish missing arrays from empty arrays, missing elements from out-of-bounds errors, and exact row counts from upper bounds. Preserve operation order, keys, logical partitions and source identity. Unsupported functionality must fail explicitly.

## Review findings and required changes

- Filter: replace repeated per-column predicate preparation with Arrow's `filter_record_batch`; it already preserves zero-column cardinality. Avoid rebuilding a mask for `keep=True`.
- Public signatures: match Hail keyword names (`filter(expr=...)`, `array(collection=...)`, `sum(collection=...)`, `product(collection=...)`) and test them explicitly.
- Filter: invalidate cached exact cardinality after filtering; do not return the construction-time count or a head/tail upper bound as an exact count.
- Filter tests: a predicate from the input Table is stale after `head`; bind the resulting Table and construct its predicate from that source. Test rejection of stale expressions separately.
- Collections: reject column-major concatenation for row-valued array construction; use DataFusion `make_array`. A two-row/two-field test distinguishes correct interleaving.
- Collections: initialize typed reduction accumulators before consuming the first element; remove per-element ScalarValue/one-element-array allocation.
- Collections: do not silently turn Hail tuples into arrays or add array-only reduction methods to every expression type. Use Hail-shaped top-level helpers.
- Collections: Hail collection sum/product use typed zero/one identities; DataFusion float64 reductions would lose large-int precision and have different empty semantics. Array aggregation across rows is a different operation and is not a valid semantic reference.
- Collections: distinguish omitted reverse-slice stop from an explicit `-1`; validate index/step types and zero steps; preserve Hail bounds errors instead of silently returning null.
- Collection review tests: corrected the reviewer’s Python-slicing assumption after inspecting Hail `Emit.scala`: reverse stop `-length` clamps to `-1`, differing from Python. Added a source-derived Hail regression instead. No live oracle claim is made.
- Test recorder: a failing upstream suite must affect the process exit code. A failed extension build must not permit passes against a stale installed extension.
- Test recorder: save results incrementally, bound subprocess runtimes, prune excluded directories before traversal, and record source hashes at both ends of a run.
- Test recorder: separate test counts, subtests, suite infrastructure failures, unsupported cases and unavailable oracle cases. Do not infer source drift during a completed run from files changed afterward.

The root reviewer added independent filter and collection boundary tests in `python/tests/test_filter_review.py` and `python/tests/test_collection_review.py`. Implementation agents do not own those tests.

## Evidence

The original baseline command logs and source hashes are preserved in `conformance/runs/20260924T203512Z-baseline`. Its original runner did not capture an end-of-run source hash. The first integrated run passed the new implementation tests and verified start/end hashes, but its recorder crashed while printing final totals. The logs are retained with a recorder-error note. The fixed recorder includes complete-main smoke tests. The [verified run](../conformance/runs/20260924T205540Z-integrated-verified/summary.json) has no source or extension drift: 25 Rust tests, 62 Python tests, 19 subtests and nine recorder tests pass. Original Hail bodies have 89 passing cases and the known full-annotate failure; native fixtures have three passes and one unsupported globals case. All four live-oracle fixtures remain unavailable. Hail oracle availability is recorded separately from source-extracted Hail test-body execution.
