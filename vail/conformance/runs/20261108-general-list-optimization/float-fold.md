# Ordered floating-point map/fold fusion

Production `0d60d0aa179d1b7490d07a4114a8557ca9d9ef46` extends the existing direct-output list-fold sink to Float32 and Float64. The supported shape is a pure map/filter/flatmap pipeline followed by a literal-seeded, accumulator-left addition or multiplication fold. Unsupported expressions retain their ordinary route. No new binding operator, CSE policy, interpreter or genotype/benchmark-name pattern is introduced.

The existing block program traverses borrowed input spans and updates final per-row scalar states rather than building a mapped/flattened list. Each row keeps emission order and its original float precision: no reassociation, widened F32 state, tree reduction or FMA. Seed bits, strict missing propagation, missing outer lists and empty-list seed behavior are preserved. Type and operator dispatch surround primitive loops; preparation and scalar-register configuration stay outside those loops. This does not add a universal flatmap or solve PL dosage's shared probability-list/index consumers.

## Matched public expression measurements

The same public Rust example is compiled normally against accepted main `945de6999` and the candidate. Fixtures and independent complete value/null/bit oracles are outside timing. Each of 12 cohorts uses four fresh processes/version in balanced B,C,C,B,B,C,C,B order; five evaluation intervals per process last at least 20 ms. Preparation, first evaluation and warmed preparation are reported separately. CPU is unmeasured.

| Pipeline | Rows × input width | Repeated evaluation speedup |
|---|---:|---:|
| F32 / F64 map→sum fold | 1 × 3 | 4.70–4.75× |
| F32 / F64 map→sum fold | 64 × 3 | 4.41–4.66× |
| F32 / F64 map→sum fold | 64 × 128 | 1.91–1.96× |
| F64 two-output flatmap→sum fold | 1 × 3 / 64 × 3 / 64 × 128 | 6.33× / 5.32× / 4.01× |
| F32/F64 map or F64 flatmap→sum fold | 1 × 8,193 | 106–122× |

The large final row highlights the old callback-at-each-position implementation; it is not a representative end-to-end Hail speedup. Warmed preparation falls 38.4–60.9%; cold preparation plus first evaluation improves in every cohort. All 96 workers validated and exited with owned groups absent; executable hashes remained unchanged. There is no claim about CPU saturation, SIMD throughput or unique CPU attribution, and no successful stack-sampling profile is claimed.

## Real installed-wheel controls

The unchanged upstream Hail benchmark bodies run on Vail's real native 1,000-row × 2,535-sample fixture with four partitions. This compares the normal installed candidate wheel to accepted Boolean-fallback production `1dad1531c`, not Hail itself or the full corpus. Each case uses eight balanced fresh workers, two warmups and five timed original force-count calls per worker. Medians below are medians of process medians.

| Workload | Before wall | After wall | Wall change | Before CPU | After CPU | CPU change |
|---|---:|---:|---:|---:|---:|---:|
| Entry filter | 51.099 ms | 50.570 ms | -1.034% | .196937 s | .194945 s | -1.011% |
| Filter/unfilter | 107.539 ms | 106.921 ms | -.574% | .406246 s | .404513 s | -.427% |
| Array arithmetic / PL dosage | 55.572 ms | 55.911 ms | +.609% | .212176 s | .212664 s | +.230% |
| Split multi HTS | 161.937 ms | 164.585 ms | +1.635% | .424628 s | .424057 s | -.135% |

The adverse arithmetic and split wall observations are retained; there is no noise waiver or attribution of their movement to a particular internal operator. These are controls, not evidence that PL dosage or split uses the new float fold sink. Root's acceptance decision must weigh these observations explicitly against the general fold gains.

All 32 workers / 160 samples passed complete Arrow values and metadata equality outside timing; split also passed the established independent reference. All 13 fixture files passed final full-content hashes, native/frontend/source/reference pins matched and all owned process groups exited. The original force-count action does not independently prove every payload field is forced.

CPU is process CPU across threads; the native runtime has 32 Tokio workers, so RAYON4/BLAS1 is not a global four-thread limit. Peak RSS includes imports and full-value validation, and a sampled 6 GiB worker guard is not a hard process-tree memory limit. No timed RSS improvement is claimed.

## Correctness and allocation

Exact candidate source passed 129 focused Rust tests and its normal maturin-installed wheel passed 524 Python tests plus 10 subtests. The sole excluded correlation allocation test is the previously reproduced baseline failure. New public oracles exercise both float widths, add/multiply, seed/null/NaN/signed-zero bits, cancellation, infinities, missing elements, missing outer lists hiding payload, empty/sliced lists, two-emission flatmaps and a 16,387-visible-lane recurrence crossing multiple 8,192-lane blocks. Each expected result is an independent stepwise fold; no private plan or buffer assertion substitutes for values.

The mapped-fold allocation test excludes fixtures, preparation and warmup. With both inputs above the workspace plateau, 16,384→65,536 input values allocate 410,706→410,706 bytes. The same public test against accepted source fails only its resource guard, at 9,572,133→38,276,901 bytes, after checking full scalar results. This proves bounded post-plateau evaluation allocation for that pipeline, not zero allocation, all-size constant scratch, RSS, arbitrary list-byte bounds or source-owner release. It also preserves the existing scratch/output accounting policy.

The first 1,024→32,768 test failed at 52,306→410,706 bytes because it crossed the existing workspace's block-capacity ramp. That failed guard is retained; production was unchanged when the test was corrected to compare beyond the plateau. A separate initially cold-workspace build was deliberately cancelled with its owned process group absent, then rerun in the established warm workspace. Exit143 was not a test result.

## Provenance

`float-fold-evidence.zip`: SHA-256 `ae7c4add2cd93651302d534164a3131bb3bd5dd610d64bfc5422dd7644bd2dd0`, 305,540 bytes, 547 payloads. Exact member set and every payload length/hash were verified. It contains source, all failures/final gates, normal Cargo/wheel identities, before/after source restoration, full oracle identities, both matched cohorts and cleanup receipts. Native/executable binaries, wheels, environments and large Arrow references are excluded, with identities retained. Private root: `/private/tmp/vail-general-list-20261108/float-fold`.

No cloud work, `.so` cache workaround, host-settings change, upstream PR, push or deployment was used. Independent exact source, resource, baseline-sensitivity and raw-evidence review is required before main integration; correctness clearance alone is not performance admission.
