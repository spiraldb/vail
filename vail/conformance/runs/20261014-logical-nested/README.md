# Logical nested aggregation

Nested array/group aggregation and folds now use the same DataFusion logical
compiler as ordinary table aggregation. The former physical relational planner
has been removed from aggregation.rs; it retains scope extraction, initializer
binding, group-key helpers and the native fold scalar compiler.

- Array aggregation uses zipped logical Unnest for elements and positions,
  per-position aggregation, and DataFusion's ordered array_agg. Ordered partial
  states retain indices when partitions merge. A native length-validation UDAF
  checks all arrays, including empty ones. Joins preserve missing and empty results.
- Nested group_by extends the logical grouping keys and collects dictionary
  entries. Independent filtered/exploded scopes join back to their enclosing
  group domain with explicit empty-input defaults.
- Fold input is ordered. Its native accumulator adopts its first partial state
  rather than applying the initializer twice during a partial/final merge.
- Runtime initializers use an explicit logical extension. Scalar parameters
  are evaluated from lazy globals before the same logical compiler plans the
  bound aggregate. The runtime boundary drains all grouped output partitions.
- Grouped schema inference uses the logical compiler. Synchronous legacy
  consumers bridge to deferred logical execution instead of rebuilding table
  aggregation with the old physical compiler. Nested scans still retain their
  existing native window kernels; this change concerns table aggregation.

Validation:

- Final dedicated regression file: **11 passed**. It includes actual native
  logical-plan assertions, random-derived row inputs, collected random value
  comparisons, recursion, grouped arrays/dictionaries, empty scope defaults,
  fold initialization, and matrix round-trip behavior.
- Focused aggregation/initialization suite: **27 passed**, before adding the
  two recursive-array parameter cases (both pass in the final file above).
- Three original upstream bodies executed with their assertions unchanged:
  expr/Tests.test_agg_array_agg_sum_vs_sum, expr/Tests.test_agg_corr, and
  table/Tests.test_to_matrix_table: **all passed**. AST extraction removes
  surrounding module imports/decorators, not assertions. The permanent local
  correlation regression uses NumPy instead of requiring SciPy in dev installs.
- **60 repeated grouped array actions** pass across 1, 3 and 7 input partitions.
- Full local Python suite plus conformance harness: **1,232 passed, 2 failed,
  1 skipped, 19 passing subtests** in 222.02 seconds. This run preceded addition
  of the two recursive-array parameter cases. Both failures are pre-existing
  file-pruning tests, previously reproduced on the requested base:
  test_interval_partition_join_reads_only_files_needed_for_selected_output and
  test_membership_join_executes_only_selected_left_partition[semi_join-expected0].
  The latter has also failed for the anti_join parameter in previous runs.
- Rust workspace (including doc tests): **205 passed, 1 failed, 1 excluded**.
  The failure is the previously reported
  distinct_rejects_float_equivalence_mismatch assertion in the unchanged
  collection_functions integration test. The excluded force_count test is
  checked separately: it still times out after 45 seconds; see rust-hang.log.

These results do not claim that the entire upstream suite passes. The existing
upstream IR-adapter limitations and fixed phased-genotype sample mismatch are
unchanged by this architecture migration.

Commands run from vail/:

```sh
PYTHONPATH=python .venv/bin/python -m pytest python/tests/test_logical_nested_aggregation.py -q
PYTHONPATH=python .venv/bin/python -m pytest python/tests conformance/test_harness.py -q
RUSTUP_TOOLCHAIN=1.98.0 RUSTC_WRAPPER='' RUSTFLAGS='-C link-arg=-undefined -C link-arg=dynamic_lookup' CARGO_TARGET_DIR=/private/tmp/vail-randomness-target cargo test --release --workspace --no-fail-fast --locked -- --skip force_count_drains_later_partitions_while_first_is_gated
```

The pre-existing force_count concurrency hang is excluded from the Rust
workspace run and reported separately, rather than allowing it to block all
remaining targets. Complete logs and source/native hashes accompany this report.
