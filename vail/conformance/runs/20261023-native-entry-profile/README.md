# Native entry computation: profiles and general optimizations

This investigation uses genuine Hail-origin fixtures converted to native Vail
storage. It does not measure the legacy reader. The baseline is the normal-built
current-main baseline from the legacy join investigation, source base `6b6eb3b6`,
native SHA256 `29b5998dc15b82534967b0ce4c749c561fbd24368259b9ef181ea13fef42ece9`.
No extension binaries were copied as a build cache. The machine is the exclusive
32-vCPU `vail-bench-20261021`; builds and timed queries do not overlap.

## Warmed measurements

Each public query is executed four times in one fresh Python process. The first
sample is excluded; the following three form these medians. CPU time is summed
process user/system time. Peak memory is process high-water RSS, not a per-query
allocation total. Profile25 is the existing native profile matrix, not the
billion-entry scaling fixture. These are diagnostic measurements, separate from
the exhaustive scan and Hail comparison.

| Query | Wall seconds | CPU seconds | Peak RSS GiB |
|---|---:|---:|---:|
| Direct matrix DP sum | 0.225 | 5.63 | 0.91 |
| Entry-table DP sum | 0.940 | 19.47 | 3.10 |
| 33 row aggregates | 1.136 | 26.56 | 1.08 |
| 33 column aggregates | 0.736 | 16.49 | 1.10 |
| Coverage counter/mean pipeline | 2.137 | 64.40 | 0.85 |
| PL dosage annotation | 1.011 | 24.13 | 5.21 |
| Global GQ stats | 0.408 | 10.28 | 0.95 |

Direct and entry-table DP sums both return `353709503` on every sample. Row/column
and coverage benchmark actions force complete result execution. PL dosage is
annotation/materialization, not aggregation.

## Confirmed opportunities

1. **Scalar integer min/max unnecessarily constructs lists.** Coverage spends
   approximately 30% inclusive in DataFusion `make_array`; source confirms
   `min(100, x)` builds `[100, x]` per entry before reducing it. The candidate
   lowers nonempty explicit integer min/max arguments directly to DataFusion's
   `least`/`greatest`. This applies to arbitrary integer clamps and multiargument
   extrema in tables, nested maps and matrix computations, independent of any
   benchmark name, size or field. Floating-point and strict-missingness contracts
   retain their current implementation until separately justified.

2. **Custom entry expansion hides projection demand.** Entry-table scalar sum
   spends significant self time in Arrow list/string gathering, offset copying
   and MutableArrayData. The custom expansion emits all row/column/entry fields;
   the scalar DP consumer discards most of them. The existing nested projection
   pass explicitly treats expansion as consuming the child's entire output.
   This needs a general projection contract, not a special sum rewrite.

3. **Key proof loss can collapse parallelism.** Explicitly selecting only DP
   reduces CPU but makes this query slower: about 5 seconds for combined narrow
   row/column/entry selection, versus 0.94 seconds without selection. Selecting
   entries alone takes about 9 seconds. `matrix_entry_partition_groups` falls
   back to one concatenated stream if inherited key bounds cannot be proved.
   Source key forwarding currently recognizes direct columns and identity casts,
   while matrix reconstruction frequently re-expresses keys through struct fields.
   Projection improvements must retain equivalent key/ordering properties rather
   than trade decoding work for serialization.

4. **Opaque sibling reductions block useful CSE.** The physical diagnostic plan
   for four fractions over different thresholds contains four different semantic
   kernels for the same entry-presence denominator. DataFusion shares repeated
   use within each fraction, but cannot identify sibling kernels as equivalent.
   Improve semantic identity/exposure instead of introducing a second bespoke CSE.

5. **Array computation is mostly framework work, not just expensive math.**
   Dosage samples include about 24.7% inclusive in DataFusion array-transform and
   substantial CASE/indexing/allocation. `pow` alone is only about 1.4% self;
   changing to a PL lookup cache is not supported as the leading optimization.
   Row/column many-aggregate samples show indexing (~6–10% self) and ordinary
   comparison/arithmetic kernels. Profiles include startup and small OpenBLAS
   background-thread samples; inclusive percentages overlap. Some DWARF stacks
   truncate at unknown frames, so these are direction-finding measurements rather
   than a complete disjoint CPU accounting.

## DataFusion integration contract

The exact pinned version is **55.0.0**, from Cargo.lock. Its optimizer sequence
includes `SimplifyExpressions`, `CommonSubexprEliminate`,
`ExtractLeafExpressions`, `PushDownLeafProjections`, and `OptimizeProjections`
(`datafusion-optimizer/src/optimizer.rs`, default rules). Scalar extrema now
become ordinary logical ScalarFunction expressions before `state.optimize`, so
those standard passes can see their arguments, constants and equality. Prepared
nested callbacks also use the existing DataFusion physical ScalarFunctionExpr;
no new runtime interpreter or custom scalar kernel is introduced.

For future entry expansion changes, DataFusion 55's
`ExecutionPlan::try_swapping_with_projection` is the physical projection hook;
the default returns None. Logical extension input/expr rewriting and supported
projection contracts must also allow required nested fields to reach scans.
Provided ordering, required ordering and distribution are separate properties:
unordered reductions must not inherit a requirement merely because the input
can provide ordering. Existing count-demand fixes do not automatically establish
this property for all aggregates.

Pinned source references:

- [DataFusion 55 optimizer sequence](https://github.com/apache/datafusion/blob/55.0.0/datafusion/optimizer/src/optimizer.rs)
- [Common expression elimination, including volatility and conditional demands](https://github.com/apache/datafusion/blob/55.0.0/datafusion/optimizer/src/common_subexpr_eliminate.rs)
- [ExecutionPlan projection/ordering contracts](https://github.com/apache/datafusion/blob/55.0.0/datafusion/physical-plan/src/execution_plan.rs)
- [Least implementation and null behavior](https://github.com/apache/datafusion/blob/55.0.0/datafusion/functions/src/core/least.rs)
- [Greatest implementation](https://github.com/apache/datafusion/blob/55.0.0/datafusion/functions/src/core/greatest.rs)

## Candidate validation

The normal kache/maturin release build completed in 1m29s. Native SHA256 is
`7f0d1fd074d799b69eb83cdab74c4810f8a1d8f49f99aa052b4cbb0b18a677ad`.
Final public execution checks: **40 passed in 1.72 seconds**, covering scalar
extrema, entry sums, coordinate aggregate callbacks and strict entry inputs.
The first test draft assumed floating NaNs were ignored; baseline execution
showed the existing scalar API propagates NaN. That test oracle was corrected;
the production floating path remains unchanged.

Matched baseline/candidate measurements use one warmup plus three samples,
`OPENBLAS_NUM_THREADS=1` for both engines, genuine native profile25/coverage inputs,
and no simultaneous workloads:

| Pipeline | Baseline wall / CPU s | Candidate wall / CPU s | Wall speedup |
|---|---:|---:|---:|
| Coverage counter/mean | 2.142 / 64.57 | 0.905 / 26.58 | 2.37x |
| Global clamped-entry sum | 1.017 / 27.17 | 0.274 / 6.76 | 3.72x |
| Row clamped sums + total | 1.105 / 28.00 | 0.294 / 6.92 | 3.75x |
| Unchanged 33 row aggregates | 1.131 / 26.62 | 1.121 / 26.02 | 1.01x |
| Unchanged PL dosage | 0.974 / 24.24 | 1.025 / 23.61 | 0.95x |
| Unchanged global GQ stats | 0.394 / 10.13 | 0.402 / 10.31 | 0.98x |

Both clamp formulations return **353404627** for every baseline and candidate
sample. Coverage peak RSS drops 0.842 to 0.674 GiB. Candidate coverage samples no
longer report `make_array` above the 0.5% inclusive report threshold; native
DataFusion `least` is about 1.23% inclusive. Inclusive percentages overlap and
must not be summed. An initial approximately 5% dosage wall-time difference did
not reproduce in three alternating baseline/candidate control rounds (nine warmed
samples each): dosage **1.032 to 1.016 s**, GQ stats **0.40165 to 0.40159 s**, and
33 row aggregates **1.149 to 1.138 s**. The corresponding CPU medians are
24.59 to 24.47 s, 10.21 to 10.40 s, and 26.79 to 26.34 s. No Hail speedup is
inferred from these new measurements.

The projection/ordering/CSE issues remain separate queued general investigations;
they are not bundled into the scalar extrema change.

Raw CPU-clock profiles, self/inclusive reports, diagnostic plans and query receipts
are preserved in `native-entry-complete-evidence.tar.gz`, SHA256
`c400d6e4817f42891647bd5ab103fc81da13083e580c19c60d0f96bb32835170`.
The archive was downloaded and independently checksum-verified before stopping
the VM. Its final test log predates the last exception-type assertion tightening;
the committed `evidence/final-tests.log` is the subsequent exact-test rerun.
