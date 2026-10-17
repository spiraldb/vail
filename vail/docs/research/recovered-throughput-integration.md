# Recovery of stranded throughput work

Integration base: `e99ad227173f4e960aa1394430c79764b05bc156`.
Recovered branch: `dk/columnar-reductions-and-reader-pushdown`, tip `5bdbb9bc` (31 commits not reachable from main).

The integration retains all recoverable source, tests, documentation and historical benchmark evidence. Historical runtime artifacts are evidence, not current measurements. No build outputs are added.

## Reconciliation

- Restored `hl.zeros` through DataFusion `array_repeat`, typed Int32 ranges and direct numeric-buffer mean kernels.
- Restored native grouped sum/count, coordinate reductions, Arrow-struct regression predictors, genotype-likelihood expressions, projection pushdown/context narrowing, reader batch limits and deferred large immutable constants.
- Integrated join range pruning/splitting and unique-key optimizations with main's prepared-input/clipping/planning machinery; preserved newer repeated-join fixes.
- Preserved main's streaming previous-nonnull implementation rather than restoring the old scalar window route. Count scans reuse zero-based source ordinals.
- Preserved native UInt16 Call fields and their constructors/accessors; omitted obsolete packed-word Python predicate code. Explicitly typed Call key layouts accept Iceberg's signed Int32 storage widening and restore typed logical Arrow values.
- Preserved main's streaming QR numerical policies rather than restoring legacy normal-equation accumulation. Predictor structs avoid rebuilding per-observation lists.
- Preserved action-scoped planning and initialization; no physical execution callback invokes a planner. Physical consumers remain single-use; separate public actions create fresh resources.

## Review fixes

Required reviewer: `01a14128-21b8-7a53-b399-4ad8f9b31f4f`.

Review found and integration tests confirmed zero-based count offsets, predictor contract mismatch/count-before-invalid handling, repeated DAG traversals, a false partition-distribution certificate, and new tests depending on private operators/mocks. These were corrected. New tests use public output/type/action boundaries and real telemetry processes. Untagged partition sets retain unknown distribution; only an explicit partition-ID column can certify range distribution.

Native aggregate validation memoizes by node identity and row-allowed context, preserving error/scope semantics. No-op coordinate/initializer/scope rewrites retain shared Arc children rather than expanding paths. The depth-32 aggregate test checks lazy relation/schema lowering; depth-12 cases execute and assert values. Complete physical optimization and depth-32 execution are not claimed.

A bounded native profile revealed the separate existing runtime semantic-kernel common-subexpression defect described in `OPEN_ISSUES.md`. Main's `CompiledPhysical::evaluate` forwarding implementation is unchanged in this recovery. No speculative thread-local evaluation cache was introduced.

## Validation evidence

Builds, full-suite attempts, bounded stall traces/profile, corrections and final results are retained under `/Users/danielking/projects/vail/vail/conformance/runs/20261016-recovered-throughput`. Final exact revision, test counts, benchmark result and approval will be appended after validation completes.
