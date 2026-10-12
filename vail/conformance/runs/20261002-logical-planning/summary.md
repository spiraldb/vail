# Logical planning verification — 2026-10-02

The final native Python extension was rebuilt with `uv run --no-sync maturin develop` after the lazy expression source and explicit global-dependency changes.

- Python: **642 passed, 1 skipped, 19 subtests passed**. Command: `PYTHONPATH=. uv run --no-sync python -m pytest -q python/tests conformance/test_harness.py`. See [log](python-suite-final.log).
- Rust table: **44 passed, zero failures**. Command: `cargo test -p vail-table --quiet`. See [log](rust-table-tests.log).
- Rust execution: **57 passed, zero failures**. Command: `cargo test -p vail-execution --quiet`. See [log](rust-execution-final.log).

Regressions cover optimized logical projection/filter/aggregation, ordered collect/take, globals with colliding field names, global-only aggregation without reading row files, checked casts after preceding filters, and lazy expression-backed sources evaluated once across partitions and reset between actions. Provider tests cover multi-file bounded AND/disjoint OR pruning and UTF8 bounds. Scoped aggregation defaults distinguish an absent group from a matching null result.

These are Vail's local suites and conformance harness tests, not a claim that every upstream Hail Table test passes. Historical failed iteration logs are retained alongside the final logs.

Remaining reader work: Vortex within-file predicate pushdown. Certified file pruning is implemented separately. Representation TODOs and physical optimizer/global-sharing follow-ups are tracked in [open issues](../../../docs/OPEN_ISSUES.md).
