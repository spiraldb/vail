# Targeted throughput followups, 2026-11-09

Three production changes preserve Hail key ordering through localization, replace serial stable sorting with parallel per-stream sorts plus a merge, and emit contiguous singleton entry spans without concatenating their payloads. Duplicate-key runs still transpose across batch boundaries, with existing certified partition grouping and spill behavior preserved. The sorter keeps its full partition/local-ordinal tie tuple through optimizer reconstruction. Native-write investigation is diagnostic only; no speculative compressor or durability changes are included.

## Targeted results

| Workload | Before | After | Wall speedup | CPU change |
|---|---:|---:|---:|---:|
| Native matrix row-key shuffle | 107.342 ms | 44.279 ms | 2.424× | -4.69% |
| Materialized integer sort, 200,000 rows | 9.882 ms | 5.318 ms | 1.858× | -35.52% |
| Hail-input grouped collect | 171.997 ms | 89.444 ms | 1.923× | -12.70% |
| Isolated entry-span show comparison | 123.618 ms | 123.752 ms | 0.999× | +0.84% |

The matrix cases use the existing 1,000-row × 2,535-sample/four-partition laptop subset. These are not reruns of the original full-suite fixtures or new comparisons against Hail. Both versions use Rust mimalloc and the same build settings. Rayon is configured for four threads and BLAS for one; the existing Tokio runtime has 32 workers. The VAIL_EXECUTION_CONCURRENCY=4 environment variable is ignored, not a runtime cap. No task-owned builds, tests or profiles overlapped comparative timing; the desktop was not claimed otherwise idle.

The first three rows compare the combined candidate with the unchanged baseline. The last row holds the final sorter, reader, allocator, compiler and input fixed and changes only entry emission. That isolated comparison shows no material show throughput benefit. The source copy removal is independently supported by the allocation counterfactual below; it is not credited with the combined show improvement.

Adverse observations remain: row read-control CPU +1.34%; metadata cohort legacy-read wall +3.32%, native-grouped wall +0.32%, native-read wall +3.95%; combined unknown-entry show/count CPU approximately +22%, take CPU +13.84%, and duplicate-show CPU +7.70%. The isolated entry change is essentially flat, so those larger combined CPU costs are not attributed to entry-span emission. No noise, universal-win or unsupported causal claim is made. These tradeoffs remain followup opportunities.

## Validation and provenance

The corrected normal release source is 46d95c159, native SHA256 2723af5a47571de26bf525e3960aa3d27e09505b31a9d5b6e979716bf564338e. It passed 299 selected public Python checks and eight Rust entry correctness/allocation checks. The known baseline Python repartitioned-multikey ScalarSubqueryExpr failure remains explicitly excluded and unfixed. An initial development revision failed duplicate occurrence order; that artifact and failure were retained, the optimizer metadata interaction was corrected, and both the exact public regression and full selected gate passed before timing.

Test-only child199dabb34 adds a complete warmup/drop before allocation counting; all production bytes remain identical to46d. The same existing full-output allocation guard rejects the old emitter (1,501,832→5,507,672 allocated bytes) and passes the final emitter (5,703,642→1,645,914). Final small-case initialization is still larger despite warmup, so no small-to-large memory ratio or fully removed initialization-confound claim is made. This is allocation volume and regression sensitivity, not peak RSS, retained-owner release or a show speedup. All64 output payload rows are checked before the resource assertion.

The isolated counterfactual source dae79e5c7/native9c88772e restores only matrix_entries.rs to the accepted earlier implementation; final Row/Hail and allocator/compiler settings are unchanged. It is a development-only diagnostic, not a production fallback. The final branch was restored exactly before integration. Existing normal baseline source1abc1ed86/native d7119f22 is unchanged.

Native-write sampling finds frequent compression/statistics stacks and durability syncs. Existing per-input statistics caches do not establish redundant same-array scans; sequence-lock samples are mostly parked. Build overlap permits stack attribution only, not a speedup/CPU-budget claim. No small justified Vail write fix was found. The native read→write path still decodes and recompresses; the report does not establish that all observed compression work is intrinsically necessary for an identity copy.

## Detailed evidence

- [Parallel sorting](../20261109-parallel-stable-sort/README.md):18 fresh workers/90 samples; actual optimized plans and stable-tie regression.
- [Hail key metadata](../20261109-hail-key-metadata/README.md):24 workers/120 calls. Physical source-order/range proofs remain incomplete; grouped legacy execution still hash-repartitions.
- [Entry spans](../20261109-entry-key-spans/README.md):30 workers/120 calls plus12 isolated workers/48 calls; all adverse controls retained.
- [Native-write profile](../20261109-native-write-profile/README.md): diagnostic call-chain evidence and limitations.
- This directory retains root artifact identities, gate logs/results, allocation counterfactual and input/native hash checks. Worker receipt scope differs across cohorts: normal subprocess completion is not independent positive process-group cleanup certification. Root build identity receipts bridge row worker paths to native hashes.

Independent reviewer: Codex chat01a14128-21b8-7a53-b399-4ad8f9b31f4f. Root alone integrates main; no upstream PR, push or cloud work is included.
