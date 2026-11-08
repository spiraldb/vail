# General list optimization: borrowed-value masking

Current worklist status, accepted changes, rejected experiments and remaining fusion gaps are recorded in [worklist.md](worklist.md). The report below retains the original matched masking cohort; its later-stage checklist is historical and superseded by that status report.

This is the first completed implementation from the general list-optimization worklist. Conditional reads of existing canonical Arrow values now change parent validity without gathering and merging their nested payload. It applies to ordinary Column/Lambda reads and ordinary Struct/Tuple field paths; no benchmark or genotype field names are matched.

## Matched real workload results

Normal installed Vail wheels on the same laptop, before `fcd760817e4898fe0455ffa35482022ccbec3f81`, after `3fac5cc13b3cf6712ddefc1aac17146860c613a6`. These are the unchanged upstream Hail benchmark function bodies running on Vail with the existing real 1,000-row × 2,535-sample native fixture (four input partitions), not a new Hail-engine comparison or the full corpus. Each case has eight fresh workers in balanced B,C,C,B,B,C,C,B order, two warmups and five timed original calls per worker. The table reports medians of process medians.

| Workload | Before wall | After wall | Wall change | Before core-seconds | After core-seconds | CPU change |
|---|---:|---:|---:|---:|---:|---:|
| Entry filter | 88.128 ms | 68.571 ms | -22.192% | .352436 | .279130 | -20.800% |
| Entry filter/unfilter | 144.640 ms | 125.712 ms | -13.086% | .564182 | .492082 | -12.779% |
| Array arithmetic / PL dosage | 55.632 ms | 55.679 ms | +.084% | .214352 | .213222 | -.527% |
| Split multi HTS | 162.384 ms | 162.804 ms | +.259% | .428063 | .424425 | -.850% |

Both adverse wall changes are retained. There is no statistical/noise waiver, attribution of small control movements, full-suite or Hail-relative speedup claim.

Complete Arrow values and metadata were checked outside timing against the pre-change outputs; split also used the established full reference. Timed original force-count bodies are unchanged; this evidence does not independently prove that every payload field is forced by that action. All 32 workers (160 timed samples) passed, exited zero, were reaped, and had positive process-group absence checks before advancing. All 13 fixture files passed final full-content hashes. Source, installed native, frontend and reference identities were checked before queries.

CPU is process CPU across threads. RAYON4 and BLAS1 were fixed, but NativeSession creates 32 Tokio workers; this was not a global four-thread limit. Worker peak RSS includes import/validation and is not a timed-memory improvement. A sampled 6 GiB worker guard is not a hard process-tree memory bound.

## Correctness and resource scope

Exact final source passed 121 Rust tests (71 library, 34 list-fusion, eight new masking, four conditional, two CSE, two semantic-preparation). Its normal installed wheel passed 519 Python tests and 10 subtests. The sole excluded correlation allocation test is the previously reproduced baseline failure, not a new exclusion.

Masking preserves existing parent nulls and treats missing predicates as false. Public tests cover sliced nested values, empty structs, JSON and native Arrow missing literals, scalar captures, lazy errors, representation conversion and large mostly-unselected nested string-view payloads. Masking allocation stays bounded when a 16-row string payload grows from 16 KiB to 16 MiB. This is allocation volume excluding fixtures/preparation; it is not peak RSS, zero allocation, zero-copy everywhere, or input-owner release.

Scalar captures, virtual field accessors and sources requiring storage normalization keep the established lazy CASE path. The actual source descriptor is certified once per binding context/root slot; prepared field indices retain their Arrow descriptors. Changed expression children invalidate the eager-read certificate and rebuild lazy CASE. The fast evaluation path has bitmap work and metadata allocation but no per-entry preparation, schema construction or compilation.

An initial candidate `4fd7c0601` recognized only JSON missing literals and did not admit the Python frontend's Arrow missing literals. Its complete no-gain cohort is preserved separately and is not evidence for the corrected implementation. All intermediate failed compiler/profile attempts are retained; only final exact gate logs are claimed.

## Native execution diagnostic

Normal-user macOS stack sampling stalled on both Python and the Rust replay. All three bounded attempts ended with the worker reaped and owned worker/sampler process groups absent; no stack profile or setup attribution is claimed. There were no permission, system-setting or interpreter changes.

A public Rust table-API replay used the actual exported private projection IR and the same native input, converting exactly representable primitive/null literals to JSON form. It checked all reference columns, complete values and exact Arrow metadata against the Python core output before profiling. Clearing keys reproduces the captured relational step; this is an operator replay, not a proof of an identical Python physical artifact or construction path. The reference Arrow file is retained privately, not embedded in the evidence archive.

Existing DataFusion metrics from a fresh action's partition-preserving full-value collection showed the complete list-transform expression evaluating in all four partitions (45.71, 52.15, 53.64 and 53.77 ms). These are inclusive expression/partition elapsed counters, not unique CPU, additive end-to-end wall time, or a before/after attribution. A preliminary generic collector interleaved partitions and failed the ordered comparison; the partition-preserving collector passed the complete unchanged oracle. This diagnostic collect differs from the timed original Python force-count calls and does not establish their payload demand by itself.

## Worklist and binding correction

1. Borrowed-value masking: implemented and measured above; final evidence review pending.
2. Binding/CSE: the current physical lowerer **already** removes singleton map/index `bind` scaffolding using an existing case-less lexical Switch. Raw frontend IR did not establish runtime singleton allocation. No new bind operator or duplicate rewrite was added. Cost-aware inlining/CSE comparison remains open; pure cheap expressions should remain free to recompute.
3. Visible logical higher-order expressions: a draft exists outside production; scoping, preparation cost and existing optimized-route coverage are under review.
4. Cross-projection map/filter/flatmap composition: open.
5. Compacting filter-map and fixed-cardinality execution: masking above handles one fixed-cardinality case; general composition remains open.
6. Producer-to-scalar-consumer fusion including floats: open. PL dosage already has an algorithmically specialized expression and physical bind normalization; its probability-map intermediate needs execution evidence.
7. Split-multi coverage/control audit: open for the remaining general rules. This masking change shows no material split-multi improvement.

No cloud work, upstream PR, push, or website deployment was performed for this step. Main integration requires independent exact review first.

## Provenance

Private raw evidence and reusable harnesses: `/private/tmp/vail-general-list-20261108`. Corrected observations are `final/` and `final-records.json`; the earlier cohort is `cohort-4fd-no-native-admission/` and `cohort-4fd-records.json`. Final source tests are `masking-runtime-null-tests.log`; the normal installed Python gate is `masking-python-receipt.json`. Earlier logs are not final gates.

Corrected wheel SHA-256: `33bb1eb33cd2caf20b120dcca3824f0e0f029fd394d0acb78585609a19420b6e`. Installed native SHA-256: `ba42d466156fedd6eb0cf4dfeacecb6b78dee2df255aac49f8f845db92c04d81`.
