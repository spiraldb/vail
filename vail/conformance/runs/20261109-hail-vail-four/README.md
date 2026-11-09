# Matched Hail / Vail laptop comparison — 2026-11-09

Four unchanged upstream benchmark bodies ran on the same logical 1,000-row × 2,535-sample public input with four partitions. Vail used its native Vortex-backed format and Hail used its native MatrixTable format. This is the local iteration subset, not the full published profile25 fixture or a full-corpus rerun.

Vail production: `30783876b24484cc80549d175faf15bab765f676`, integrated main `17ae75d9839198a7a5b25190bb6f84a8895727b8`. The normally installed native SHA is `16a064a0650ba8482b95aa4e493237d6f26bc0dd6d85fae534237bd8fccab73a`. Hail is `0.2.139-5e5eedb87ff8`, Spark 3.5.9, configured Java 11 runtime. No production change, build, package installation, cloud work or local workload overlap was used for this comparison.

## Matched results

| Workload | Hail wall | Vail wall | Hail / Vail wall | Hail observed CPU | Vail observed CPU | Hail / Vail observed CPU |
|---|---:|---:|---:|---:|---:|---:|
| Filter entries | 192.382 ms | 50.513 ms | 3.809× | .947076 s | .213479 s | 4.436× |
| Filter/unfilter entries | 222.532 ms | 107.946 ms | 2.062× | 1.007314 s | .424955 s | 2.370× |
| Array arithmetic / PL dosage | 227.389 ms | 56.472 ms | 4.027× | 1.005116 s | .211951 s | 4.742× |
| Split multi HTS | 548.707 ms | 176.770 ms | 3.104× | 2.508146 s | .444926 s | 5.637× |

These are medians of process medians. Each case has four fresh processes per engine in fixed H,V,V,H,H,V,V,H order, three warmups and five timed calls per process: 32 workers and 160 timed calls. The original read/query-construction/force-count body is timed. Engine/JVM initialization and separate validation are outside timing. There was no adaptive warmup, favorable-worker selection or benchmark retry. Per-process ranges and every sample are retained in `analysis.json` and the raw packet.

CPU includes precise Python process CPU plus observed stable live-child endpoint CPU, including Hail's JVM. Unobserved short-lived descendants can be missed and child endpoints are not atomic with the wall timer. These are ratios of observed measurements, not guaranteed bounds on the ratios. Hail uses Spark local[4]; Vail uses RAYON4 and its existing 32-worker Tokio runtime. Both inputs have four partitions, but this is not an enforced global four-core/thread-parity experiment. Peak RSS includes imports, JIT, warmups and validation; no operator-live-RSS improvement is claimed. Recorded page-fault counters are not allocator or page-fault CPU-time attribution.

## Input and result validation

The previous native fixture's 13 data/metadata files and source VCF were absent at enrollment. The existing Hail fixture's exact 70-file roster and full content hashes still matched its frozen manifest. A fresh native copy was generated with Vail's public `MatrixTable.read_legacy`, repartitioned to four without shuffle, and written natively; dimensions and partition count pass. Its new 13-file roster is pinned. No old native timing ratio is transferred to this regenerated layout, and the old input path was not modified.

Eight separate final validation workers pass before timing. Vail's full Arrow values and metadata for all four outputs match the established pre-change references. Hail matches entry-field counts, sums, extrema, nulls, list lengths and Call summaries. Hail's 1,885,857 defined dosage values additionally match the reference as a full sorted multiset, with maximum absolute difference 4.440892098500626e-16 and total absolute difference 2.643964764465455e-11. The multiset and summaries do not prove row/column association or full cellwise Hail equality.

An initial fixed-relative-tolerance aggregate check failed: dosage sums 468730.07811905805 versus 468730.0781185798. The failed gate is preserved and no timing ran under it. The final validation separately checks every sorted dosage value at relative/absolute 1e-12 and uses a finite, nonnegative-sum reduction-model allowance for the aggregate: measured multiset L1 difference plus `gamma_n/(1-gamma_n) × (abs(sumA)+abs(sumB))`, with actual defined count `n` and binary64 unit roundoff `2^-53`. This is a numerical summary acceptance model, not a proved end-to-end/pow error bound or identification of a unique cause.

All 83 input files passed full post-timing hashes; native and JAR identities remained unchanged. Every final validation/timing worker exited with positive owned-process-group absence. The sampled combined 6 GiB RSS guard is not a hard process-tree memory limit. Worker deadlines are 180 seconds, with the 1,800-second worker phase reserving 20 seconds for bounded cleanup before each launch; full post-run hashing is a separate 60-second checkpoint.

## Separate PL layout diagnostic

The user-proposed alternative was measured after the matched cohort finished. It binds the original PL field, extracts indices 0/1/2, applies the same checked probability formula separately, and evaluates the same ordered dosage recurrence. The current implementation maps the PL child values once before extracting those probability indices. There is no sorting in either diagnostic form.

| Form | Wall | CPU |
|---|---:|---:|
| Current probability-list map | 55.103 ms | .207371 s |
| Direct three indexed probabilities | 92.586 ms | .328610 s |
| Direct form change | +68.023% | +58.465% |

Eight balanced fresh Vail workers use three warmups and five timed calls each. Both forms pass exact full Arrow values and metadata against the same reference in every worker. The timed upstream body differs only in the dosage callee; preparation is included. Domain checks outside timing establish that every defined PL has width three and values at least −2048, with missing lists allowed. The producer is an ordinary field read. The direct form lacks general wrong-width/error-order admission; it is a supported-domain experiment, not an optimizer rewrite. All eight workers and 40 samples pass; all groups are absent and the 83 input hashes still match afterward.

Retain the current mapped implementation. The direct form is materially slower in this planner/runtime; cache, gather, kernel-dispatch, conditional and preparation contributions have not been isolated. This is not evidence that scalar extraction is inherently inferior or that arbitrary shared-producer recomputation is acceptable. No production PL rewrite is enabled.

## Evidence and review

Exact final protocol SHA: `474d540012cc20be192c5d7229de54e9cf64354ee95f3c0a2d4cc0f382826615`. The final validation gate links its exact protocol, pins and all eight result/identity/exit hashes. Earlier completed validation and failed setup/gate attempts remain separately labeled. A controller signal-inheritance issue was corrected before timing; normal/deadline/repeated-signal tests verify unblocked child termination signals and clean groups.

`evidence.zip`: SHA-256 `e20857e9f0b7c3a081bca0ff2db5a8a963d4d89438dffa2527bb238eb6817be7`, 1,945,991 bytes, 424 payloads. Exact member set and every length/hash verify. The packet contains scripts/protocols, package/input/reference identities, failures, validation, raw worker records, cleanup, independent-recomputable analysis and the PL diagnostic. Fixtures, large Arrow/NumPy reference payloads, installed packages/JAR/native binaries and Hail temporary data are excluded; identities are retained. Private root: `/private/tmp/vail-hail-four-20261109`.

Independent reviewer thread `01a14128-21b8-7a53-b399-4ad8f9b31f4f` has accepted the exact final protocol, final validation gate, raw four-case results and PL diagnostic. Final durable evidence/report review precedes main integration. No website update, push or PR is part of this task.
