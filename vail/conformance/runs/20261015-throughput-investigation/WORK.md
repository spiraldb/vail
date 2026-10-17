# Throughput investigation — 2026-10-15

User requested local profiles, broadly applicable straightforward fixes, regression tests, and before/after verification. Use Luna agents; serialize heavy profiles, timings, and builds. Do not rerun Hail for cases already known to fail/unsupported in Vail.

Baseline engine wheel: `/private/tmp/vail-matrix-sum-source-wheels/vail-0.1.0a1-cp311-abi3-macosx_11_0_arm64.whl`, extracted at `/private/tmp/vail-profile-baseline-package-20261015`. Native SHA256 `e60c32b38c7d1ad3bb5a8e76c7cef5f0ee7eb9857d075abf8c9476009f963357`. Source-directory `_native.abi3.so` is stale Oct14; do not use it as current baseline. GCP a965c01 completed; VM verified TERMINATED, monitoring paused; full artifacts in neighboring 20261015-gcp-full-upstream.

## Ownership / order

1. Nested annotation physical-planning explosion — agent profile_nested_matrix_annotation owns logical.rs and planner tests. Current baseline logical plan contains40 unnecessary globals cross joins. Stalewheel sample suggested EnsureRequirements; repeat short currentwheel sample. Patch skip unused globals on rowmaps/filters; review ensured empty binding schema/offsets and actual collect assertions. Preserve scans/RNG bindings for now.
2. Matrix sex-specific QC and row-wise aggregates — profile_slow_matrix_aggs owns matrix aggregation lowering/kernels. First profile slot. Profile exact latestwheel, not deterministic substitute unless explicitly labeled diagnostic.
3. Previous-non-null scan — profile_slow_scan owns scan execution. Second profile slot45s. DataFusion native lag/last_value fastpath exists; profile before fixing.
4. Linear regression — parent owns advanced_numeric_aggregates.rs. Third profile slot45s. Existing updater allocates perrow predictorVec and slices lists; investigate measured contribution before changing.
5. Then continue remaining slow benchmark families from full report, prioritizing largest wall-time gaps/timeouts and general planner/kernel fixes. Recheck other families after shared fixes before duplicating changes.

## Validation

Use `samply record` through require_escalated; sandboxed sampler fails Unknown(1100). No signing changes needed. No overlapping heavy workloads for timing. Coordinate release builds via parent using existing `/private/tmp/vail-native-types-target`. Preserve baseline package; use separate afterwheel extraction. Do not commit agent changes until parent review/tests. No VM currently running.

## First release candidate

Built `/private/tmp/vail-profile-after-package-20261015` (native63e0bde7..., full hashes in nested current-wheel BUILD.md). Numeric aggregation suite18/18 passed with pytest `-o pythonpath` pointing to package, not stale source native. Current nested plan40→0 CrossJoins,125566→61886chars,447→247lines. Actual after action pending. Scan patch recognizes nullRuntimeLiteral so nativeprevnonnull path survives initialization. Matrix context pruning narrows nestedentry/column fields before zip/filter/map; missingancestor/fullstruct semantics covered. Regression predictors now Arrowstructchildren; legacylist slices borrowed, scratchoncebatch. Source patches uncommitted pending measuredresults/review.

After workload CPU queue: nested annotation (max60s sample), scan (tests then exactafter), matrixQC/manyaggs (tests then profiles/timings), parentlinreg after. No Hail reruns.

## Continuation: reader batch bound

The 32-worker regression probe exceeded its 16 GiB RSS guard. This is not a successful parallel speed measurement. Source analysis found that Vail's custom source operators omitted DataFusion's normal `BatchSplitStream` boundary; coordinate aggregation expands each source row across 75 coordinates. Added DataFusion batch splitting to `PartitionSetExec` and indexed reads using the session batch size. The regression executes Vail's operator on an oversized test source and verifies row order, row preservation, partition identity and batch size. Static review of the globals/unkey/total-fanout patches found no concrete P1/P2 defect; runtime validation remains required.

The temporary build target and extracted packages from the earlier measurements no longer exist. A release rebuild uses the repository target, with log `rebuild.log`. Recovered the preserved after-fanout wheel to `/private/tmp/vail-profile-fanout-recovered-20261015`. Downloaded the exact upstream many-int fixture into ignored `local-data/upstream-benchmark-20261015`; source fixture must be regenerated before comparing regression timings. No new benchmark timings yet.

Region-memory runner now rejects missing/mismatched packages and uses external hard guards (a Python signal handler cannot interrupt native execution). Initial continuation profile attempt imported stale editable native because the package root was missing; it errored at construction and is not a valid benchmark profile. Static DataFusion 55 analysis identifies the per-element `UInt64` repeat indices and dense list-scalar broadcast as allocation sources; measure with the validated release before choosing an implementation.

Current rebuilt native SHA256: `c1b6985899640fb9349977ebf1319c175ae519982ec9fdec7025c06c53418679`. Focused Python run: **136 passed in 28.59 s**, log `current-python-tests.log`, both PYTHONPATH and pytest pythonpath override set to the extracted current wheel. Reader native regression is compiling separately.

Coordination requested by user: chat `01a13be8-9fbb-7720-ad70-8b3f9f639c58` owns shared reducers, stream aggregate scans, VCF sink, logical planner migration and kernel identity/volatility on newer base `2c02020`. Sent explicit ownership split and request to reconcile existing small patches; hold additional reducer/planner edits here until confirmed. This branch keeps benchmark/profiling work and reader batch bounds; existing narrow throughput fixes need handoff/integration, not competing rewrites.

The other chat acknowledged the ownership split. None of this branch's narrow throughput patches are integrated there yet. It owns S001–S005; its S003 shared reducer implementation is expected to supersede this branch's Python Boolean reducer patch. Keep the columnar regression payload/kernel independently integrable: committed as `a446a6fcdcb01f3913517938177df3c7c497dbc1` and sent for integration. Native-reader batching remains here. Subsequent frontend/native-boundary performance measurements should use its newer native-handle code with a frozen matching Python/native package. Requested that snapshot explicitly.

Native validation complete for reader and scan unit regressions: **55 passed**, `native-unit-tests.log`; reader focused test also passed independently in `reader-batch-test.log`. The first native test build was cold relative to the maturin release dependency flags and took 10m19s; subsequent unit execution took 0.09s. Reader source/test commit: `29e28d3e2594e0490f11ee9db4dbdb3c72c44015`. Previous-nonnull commit: `fc7bd2640d5cf25f9ed3c79773bf33b5d5ae2dd2`. Narrow matrix context commit: `5b26609164be02cf237cc73511234b9f2c0d5099`. Native integration planning tests are building. Exact published many-int fixture import/write completed in ignored `local-data/upstream-benchmark-20261015/vail/many_ints_ht.ht`.

## Mandatory external review and fixes

User requires review from chat `01a14128-21b8-7a53-b399-4ad8f9b31f4f` before any main integration. No main commit/merge has occurred. Source review reported six P2 test acceptance blockers and no demonstrated production regression. Replaced helper/operator/explain assertions and matrix reducer spy with actual executed semantics. Reader coverage now uses real written Viceberg data and the public prepared-table execution API; configurable reader batch limits, exact rows/partitions and cross-batch scan state are covered. For filtered reads the DataFusion residual filter may coalesce final output, so a runtime splitting metric proves splitting work without asserting a final filtered-output cap. Reviewer accepted the existing bounded external resource benchmark plus semantic tests, without a new statistics API. Re-review required.

Linreg selects predictor representation once per batch, validates struct width/children, preserves schema order and shares observation accumulation. Projection path grouping now indexes fields once rather than rescanning paths for each field. Filtered-reader row offsets are computed once per scan instead of summing every preceding partition at every execute, eliminating quadratic metadata work over all partitions. All are pending final validation and re-review.

Measured bounded-input regression: 5.9666 s,63.117 CPU s,10.578 average cores,3.746 GiB RSS. Exact regenerated published input verified7,500,000rows/1sourcepartition. Prior local columnar-only run29.777 s/31.881 CPU s. Instructions437.5B→427.7B andcycles119.7B→121.0B; increased reported CPU time is not evidence of duplicated algorithmic work. Results/profile/10 Hz samples are in the neighboring linreg-throughput directory.

## Final package validation and latency followup

Final pre-latency-fix wheel native SHA256 `b5ec383b89f1bf1c925f288fbbe6b3c7e90dcfd7e9d327e8d5b41ffdb215af95`; superseded Python Boolean reducer patch removed. All **137 affected Python tests passed in105.47s** (`final-python-tests.log`). A second run excluding the slow row-aligned test passed136/136 in42.25s. Short stack sample of the slow test showed recursive expression/JSON cloning; it completed, so this is not a new failure. Other architecture chat owns replacing that boundary. Public native coverage:7 planning,1 lambda/global and2 real-file reader tests passed; final reader rerun includes actual scan batch-splitting assertion.

Exploratory ABBA latency over known0/1/32/1024row single-partition sums: before medians3.17–3.81ms, protected32-worker after8.10–9.19ms, +4.70–5.50ms. Shared host load; raw phases and hashes retained in `latency-comparison/`. This fails the declared diagnostic1ms overhead threshold. Implementing one centralized fanout decision that excludes known inputs fitting one DataFusion batch, applied to both session target and protected repartition insertion. Large/unknown inputs keep fanout. Reviewer independently caught an intermediate incomplete edit that changed only the target helper; it is being corrected before remeasurement. No main integration.

Corrected final native SHA256 `180f6f25bf286f96fd346450d9d2fcbc54d8903b5e705827527667d6ce6cafd9`: all137 affected Python tests passed153.13s. The160-repetition-per-phase ABBA repeat passed all four declared median-overhead thresholds; detailed results and shared-host/tail caveats in `LATENCY.md`. Mandatory reviewer chat granted final acceptance for this isolated feature patch, including all source/test fixes. Main integration remains conditional on reconciliation and validation against the incoming native-handle/shared-registry planner. No main merge occurred.

The next large-array profile initially exposed harness overhead: logical-plan formatting expanded a100M-element zero literal into a300MB diagnostic string. That interrupted profile is invalid as query evidence. Removed plan formatting, corrected guard PID parsing and moved the wall deadline before query construction; the corrected runner samples10Hz counters. Rerunning the actual query separately.
