# Randomness without structural IR assertions

A temporary copy of the pinned upstream tests omits calls to
`assert_contains_node` in the Table and MatrixTable test modules and the
Table repartition test's diagnostic `print(t._tir)`. All other assertions and
test bodies remain. The repository's tests and implementation are unchanged.
The existing isolated-body adapter still adapts imports/decorators/fixtures.

For the same 131-definition randomness selection:

- 104 passed.
- 19 had failed or blocked variants.
- 8 workers stopped at the unsupported Hail JVM `choose_backend` import.

The latter eight are harness blockers, not proven RNG failures.
`test_stream_randomness` still constructs Hail IR directly (`ir.NA`); removing
assertions cannot make that API exist.

The remaining behavioral/API failures include Table sampling, MatrixTable row
sampling, nested array aggregation over random-derived inputs, missing
balding_nichols_model/experimental/import/statistical APIs, encoding flags,
Unicode string compatibility, and worker exception reporting.

Aggregate isolation on the current native extension:

- `agg.array_agg` and `agg.array_sum` work over deterministic arrays.
- `agg.collect` and ordinary scalar `agg.sum` work over random arrays.
- `agg.array_agg`/`agg.array_sum` over random-derived arrays fail during planning.
- Even an unrelated random scalar column before aggregation of a deterministic
  array triggers the same error. An upstream `hl.scan.count()` column also
  triggers it.

This implicates scan/row-UID planning, not correlation arithmetic or boolean
sampling: nested aggregation uses `aggregation.rs::lower_group`, which calls
`plan::lower` directly. That physical MapRows path lowers expressions as scalar
projections. The RNG row UID is represented as an aggregate-count marker, which
is handled by the logical lowering path but rejected as a scalar aggregate in
this path. Nested aggregate inputs need the appropriate scan/UID lowering.

See `results.json` for per-definition outcomes and the numbered JSON/log files
for full traces. `run-audit.py` records the temporary transformation and commands.
