# Numeric temporary reuse: measured, held off main

Two prototypes keep ordinary logical arithmetic visible to DataFusion and attempt safe reuse of uniquely owned primitive value buffers. They introduce no binding operator, evaluated-value cache, field-name specialization, reassociation or FMA. Native integer construction retains its explicit wrapping policy; the ordinary DataFusion rewrite admits only floats because the binary expression's integer overflow policy is private.

The final source, `58f00949514ae902fbe285d97906f81f924eb1a2`, keeps the first operation on borrowed Column/LambdaVariable/literal inputs on its original kernel path. Later arithmetic may consume a computed temporary through Arrow's safe mutable-buffer ownership API; shared/offset storage falls back to a new value buffer and validity remains immutable. Kernel/type/operator preparation is outside primitive loops. Lambda rebase typing is bounded and the late rewrite memoizes unique expression nodes within each invocation; this is not a query-global linearity claim.

## Real installed-wheel controls

This is a laptop comparison against accepted main `945de6999` / Boolean-fallback production `1dad1531c`, not a new Hail-engine or full-corpus run. The original upstream force-count bodies, real 1,000-row × 2,535-sample native input, four partitions, balanced B,C,C,B,B,C,C,B process order, two warmups and five samples per process are unchanged. Values and metadata are checked outside timing against complete pre-change Arrow references; split also uses the established independent reference. The action does not independently prove every payload field is forced.

| Workload | Before wall | After wall | Wall change | Before CPU | After CPU | CPU change |
|---|---:|---:|---:|---:|---:|---:|
| Entry filter | 50.815 ms | 51.230 ms | +.818% | .195477 s | .199114 s | +1.861% |
| Filter/unfilter | 107.067 ms | 108.052 ms | +.920% | .406463 s | .410486 s | +.990% |
| Array arithmetic / PL dosage | 55.451 ms | 55.750 ms | +.538% | .214465 s | .214086 s | -.177% |
| Split multi HTS | 163.012 ms | 160.865 ms | -1.317% | .424608 s | .426009 s | +.330% |

All adverse observations are retained. There is no noise waiver or universal performance claim. CPU is whole-worker process CPU across threads; the runtime has 32 Tokio workers and RAYON4/BLAS1 is not a global four-thread cap. Peak RSS includes imports and full-value validation; the sampled 6 GiB worker guard is not a hard process-tree memory limit. All 32 workers / 160 samples validated and exited with owned groups absent. All 13 input contents were fully rehashed afterward.

## Synthetic costs and allocation

Each prototype has its own 120 fresh-worker balanced cohort: identity, borrowed, temporary, shared-input and real ArrayMap programs, at 1, 16 and 131,072 values. Complete independent Arrow oracles precede timings. Warmed preparation, cold preparation plus first evaluation, and repeated evaluation are separate; CPU is not measured.

Initial `4eb463620` saved large temporary evaluation time (8.65%) and mapped evaluation (5.01%), but made tiny borrowed operations 40–44%, temporary operations 37–41%, shared-input operations 21–23% and maps 19–20% slower. That policy was rejected.

Final `58f` improved repeated temporary evaluation by 36.7–36.9% for tiny inputs and 9.47% for the large input, and mapped evaluation by 15.3–17.3% tiny / 5.49% large. Borrowed controls remained +2.52%, +.29% and +.07%; shared-input controls improved .13–2.16%. Preparation still costs more: warmed map preparation +3.70–13.63%, tiny temporary preparation +5.17–8.33%, cold preparation plus first evaluation +10.92% for a one-value temporary and +20.76% for a 16-value map. Faster repeated evaluation does not erase those costs.

Public resource tests, excluding fixture creation/preparation and warmup, reduce 262,144-value nullable Float64 arithmetic allocation from 6,292,488 to about 2,098,000 bytes, including the corresponding real lambda-frame ArrayMap case (6,293,599 to about 2,099,000). This is allocation volume, not peak RSS, compact backing storage, input-owner release or zero allocation. The accepted-baseline counterfactual fails exactly those two guards while preserving value oracles. A separate preparation regression counterfactual grows a shared expression graph from 1,278,648 to 191,109,816 allocation bytes. `revision-values.log` measures corrected final-production `58f` at 2,300 to 4,676 bytes for those depths, with native/map arithmetic at 2,097,832 / 2,098,943 bytes. Earlier `4eb` measured 2,988 to 6,036 bytes and arithmetic at 2,098,128 / 2,099,239 bytes. The final 138-test gate repeats all guards without exposing their captured allocation output; the separately captured measurements have the same production source bytes.

Final exact source passed 138 focused Rust tests and a normal maturin-installed wheel passed 524 Python tests plus 10 subtests, with the sole previously reproduced correlation resource exclusion unchanged. New tests exercise eligible computed temporaries, I32/I64 wrapping, F32/F64 stepwise bits and operand order, nulls/slices/empty inputs, retained owners, lambda rebase, changed-child fallback and scalar results. Standalone borrowed arithmetic tests remain controls rather than evidence that reuse occurred.

## Disposition and provenance

**Performance hold; no numeric runtime merge into main.** The allocation improvement and isolated kernel gains do not justify the broader preparation policy and real filter/unfilter regressions for this worklist. The existing bind/CSE policy remains unchanged. PL dosage's shared probability-list/scalar-index fusion is not solved by this candidate.

Raw archive `numeric-evidence.zip`: SHA-256 `c5a48eccf3b515397507551f791d6188194a38e767743a26ae4b901c4856a898`, 495,089 bytes, 993 payloads. Every payload length/hash and exact member set was verified. It retains both adverse and final cohorts, source snapshots, normal Cargo build receipts, counterfactual failures, exact source restoration, normal wheel identities, gate logs, oracle identities and cleanup receipts. Binaries, wheels, environments and large Arrow reference tables are excluded; identities are retained. Private root: `/private/tmp/vail-general-list-20261108`.

The initially wrong-workspace build and writable-uv-cache correction are retained as setup failures/cancellations, not workload evidence. No cloud, host-settings change, `.so` cache workaround, push or PR was used. Independent source/resource, raw-counterfactual and final evidence reviews are recorded separately; source correctness clearance is not runtime admission.
