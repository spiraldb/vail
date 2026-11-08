# General list optimization worklist

This records accepted runtime changes, measured rejected alternatives and the parts still open. It is not a claim of universal list fusion, completion of the older 95-item audit, or a new full Hail-versus-Vail benchmark run.

## Accepted changes

| Change | Purpose | Measured scope |
|---|---|---|
| Borrowed conditional masking (`3fac5cc13`) | Change validity on canonical borrowed values rather than gather/merge nested payload | Entry-filter wall −22.192%, CPU −20.800%; unfilter wall −13.086%, CPU −12.779% against its preceding matched wheel |
| Boolean missing fallback (`1dad1531c`) | Lower pure exact-shared Boolean missing fallback to an existing null-safe comparison | Additional matched filter wall −26.429%, CPU −29.958%; unfilter wall −14.939%, CPU −17.708% against accepted masking |
| Ordered floating-point fold sink (`0d60d0aa1`) | Extend the existing map/filter/flatmap-to-scalar sink to F32/F64, preserving strict order/precision | Synthetic small/medium map/flatmap-fold evaluation 1.91–6.33×; long-list old-callback cases 106–122×; no actual PL/split gain established |
| Map followed by one index (`30783876b`) | Evaluate only the demanded mapped element for a proved total, strict, single-use primitive callback | Synthetic direct 1.91–26.47× and nested 1.39–14.54×; growing child allocation stays 1.7–2.8 KB instead of reaching ~4.2 MB |

Each change has its own matched baseline and raw evidence. These stage percentages must not be multiplied into a claimed cumulative speedup. Detailed reports retain every adverse control and all preparation costs: [masking](README.md), [Boolean fill](boolean-fill.md), [float folds](float-fold.md), [map-index elimination](map-index.md). The map-index acceptance explicitly includes +.732% filter wall, +.397% arithmetic wall and the documented tiny/fallback preparation costs.

## Binding decision

**No new bind operator or evaluated-value cache is enabled. Existing CSE remains unchanged.** The old singleton-map/index binding scaffolding already lowers to a lexical frame. The experiments compared cheaper recomputation, borrowed-read inlining and shared text evaluation; improvements on selected repeated expressions did not justify their preparation costs and near-flat/adverse actual controls. Pure expressions remain free to recompute. See [binding versus recomputation](cse-cost.md).

The accepted map-index rule also avoids a binding: a proof of exactly one strict parameter use allows substitution without duplicating an expensive indexed read. It declines shared uses instead of forcing once-only execution.

## Seven-item status

| Item | Current status | Remaining work |
|---|---|---|
| 1. Borrowed conditional masking | Implemented, independently reviewed and integrated | Noncanonical/virtual/scalar sources retain the established lazy path |
| 2. Bind versus cost-aware CSE/recomputation | Audited and measured; proposed policies rejected | No evidence justifies a new default policy or bind operator |
| 3. Expose ordinary HOF expressions to DataFusion | Two functioning prototypes measured, held off main | Remove duplicate lowering/type/preparation costs before enabling a bridge; preserve existing optimized routes |
| 4. Compose across relational projections | Typed DF prototype composes the verified single-use map chain, held off main | Preparation and tiny-filter regressions remain; mixed filter/flatmap and shared/effectful consumers need a measured admissible design |
| 5. Efficient fixed-cardinality/filter-map output | Canonical fixed-cardinality masking and Boolean paths improved | Blanket direct-sink replacement for ordinary map/filter was rejected; general compacting filter-map remains open |
| 6. Producer-to-scalar-consumer fusion | F32/F64 ordered folds and proved single-index maps implemented | Shared/fallible probability-list consumers in PL dosage are still not fused; blanket numeric reuse and row tiling rejected |
| 7. Split-multi coverage and controls | Actual unchanged split body included in every runtime candidate's matched control cohort | No new large end-to-end split improvement or new Hail-relative speedup established by this worklist |

## Held designs and why

The native-IR/DataFusion roundtrip prototype (`7d27899e4`) passes its focused correctness gates, but warmed preparation costs 30–74% more. Applying the direct sink indiscriminately makes the measured large filter-map 3.97× slower and identity map 39.77× slower. It is not enabled.

The direct typed DataFusion prototype (`4cb0bbd6a`) avoids that roundtrip and verifies a single physical transform for the composed map chain. It passes 133 Rust tests. Its 96-worker synthetic cohort still shows warmed preparation +23–135% and tiny filter-map evaluation +19–26%. One physical transform alone does not prove low allocation or better total throughput; its arithmetic still creates temporary arrays. It is not enabled.

Safe numeric temporary reuse (`58f009495`) reduces selected arithmetic allocation and repeated kernel time, but preparation/one-shot costs remain and actual filter/unfilter CPU rises 1.86%/.99%. It is held off main. See [numeric reuse](numeric-temporary-reuse.md).

Scalar tiling uses the same prepared expression in bounded row blocks and includes final output assembly. All six measured cases regress: probability programs +6.59–7.93%, polynomial +10.79–12.59%, cheap controls +69.78–84.85%. It is rejected. See [tiling](scalar-tiling.md).

The next viable implementation must avoid a second native-IR compilation/preparation pass, preserve typed lexical/effect boundaries and sharing, and eliminate intermediate output buffers rather than merely replace nested operator objects. A blanket binder, direct sink or tiled evaluator is not a substitute for that work. This remains an explicit runtime gap.

## Validation and integration

The latest accepted production candidate passes 137 focused Rust tests and a normal installed wheel passes 527 Python tests plus 10 subtests. The sole preexisting correlation resource exclusion is unchanged. Tests include public value/null/metadata/error/effect/bounds oracles and resource tests that fail on the corresponding accepted baseline. Source, normal wheel/native/frontend, inputs and references are pinned. Builds/tests never overlap timings.

Runtime changes require exact independent review by thread `01a14128-21b8-7a53-b399-4ad8f9b31f4f`, explicit admission of measured adverse controls, and root-only main integration. The user's unrelated checkout and the authoritative main worktree's 38 dirty/untracked paths are preserved byte-for-byte. No cloud work, native-library copying, host-setting change, push, PR or website update is part of this worklist execution.
