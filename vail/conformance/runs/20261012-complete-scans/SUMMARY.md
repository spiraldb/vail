# Completed scan extensions — 2026-10-12

Pre-scan checkpoint: `7689173d38233665fada383aaa3a6a45b8ecfc96` (`main`).
Additional scan changes are intentionally uncommitted.

Implemented configuration-first initialization, additional statistical/genomic
reducers, nested explode/group/array/filter scopes, counter, fold, private
previous-nonnull/densification helpers, and current-row result projections.
Execution uses DataFusion windows, aggregation, grouping adapters, joins, and
ordinary projections, with Arrow accumulator state. Array lengths are validated
across all partitions, including the last. Scan output remains exclusive and
retains real execution partitions, including empty ones.

## Validation

- `PYTHONPATH=python uv run --no-sync python -m pytest python/tests -q`:
  **695 passed, 1 skipped, 19 subtests passed**.
- `cargo test --offline -p vail-table -p vail-execution`:
  **109 passed, 0 failed** across unit/integration tests.
- Native extension rebuilt offline with `maturin develop --skip-install`.
- Nine original upstream cases passed: expression `test_scan`,
  `test_scan_filter`, `test_scan_explode`, `test_scan_group_by`,
  `test_scan_array_agg`, `test_densify_table`; Table `test_scan_filter`,
  `test_scan_tail`, and `test_keys_before_scans`.

This is not a rerun of the entire upstream Table inventory. MatrixTable,
randomness, and previously deferred helpers are excluded. `approx_cdf` remains
explicitly unsupported as documented. Approximate quantiles use the existing
library and retain its documented limits. Hail's unstable statistics formula is
preserved; see [live observations](hail-oracle-observations.md).

Three Luna reviews plus parent review found and fixed a nested-fold alias
collision and strengthened regression coverage. See the
[review disposition](../../../docs/reviews/20261012/SCAN_SUMMARY.md).
