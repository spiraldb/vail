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

Builds, full-suite attempts, bounded stall traces/profile, corrections and final results are retained under `/Users/danielking/projects/vail/vail/conformance/runs/20261016-recovered-throughput`. Final validation is performed against source revision `743ea82b31d4764b638c30de36c7e03618606fe6`. The matching native extension SHA-256 is `1ddf47e534b32cc20d3f534a03a2ae07035dedbe531eddc0c1e363ef80d57cd9`. Final counts and approval follow below.

The reader-batch regression also found that caller-owned sessions inherited DataFusion's default round-robin repartition rule, unlike Vail's ordinary session. Caller planning now applies the same ordering policy while retaining the supplied runtime, memory/spill configuration, and batch size. The actual filtered reader certifies its explicitly emitted partition-ID column. Both focused reader cases pass. Earlier assertion failures and the final full-build disk-space failure remain recorded; the latter was resolved by deleting regenerable test executables and limiting linker concurrency.

Final matching validation:

- Python: 1,854 passed, 1 skipped, 19 subtests passed (49.88 seconds), `python-tests-final-candidate.log`.
- Rust: 282 passed, 0 failed, 1 ignored across 45 test binaries for vail-table, vail-execution and vail-storage, `rust-tests-final-confirmed.log`.
- Benchmark runner tests: 6 passed, 2 Linux-only telemetry tests skipped on macOS, `benchmark-tests-final.log`.
- Unmodified upstream `test_test_inner_join_region_memory`: 3.024937 seconds, 5.045631 core-seconds, 24,663,293,952 bytes peak RSS, with 10 Hz telemetry. Saved Hail runtime is 3.340630 seconds, ratio Hail/Vail 1.104; this compares laptop Vail with the earlier GCP Hail measurement and is not a same-machine performance claim. Evidence: `conformance/runs/20261016-inner-join-current-main` in the local repository checkout.

Review source findings have been addressed and cleared through the exact source revision above. Final integration disposition: **APPROVED** by reviewer chat `01a14128-21b8-7a53-b399-4ad8f9b31f4f` for exact source `743ea82b31d4764b638c30de36c7e03618606fe6` against `e99ad227173f4e960aa1394430c79764b05bc156`, plus this documentation-only record. The reviewer inspected source and evidence, ran no builds/tests, and made no edits. All verified review findings were addressed. The benchmark is a concrete resource observation, not bounded-memory acceptance or a same-machine Hail speedup; historical artifacts do not validate every recovered optimization. Runtime DAG reevaluation, initialized/coordinate rewrite limits, and wide scan lookup costs remain open. Depth-32 lazy/type coverage does not claim depth-32 physical execution.
