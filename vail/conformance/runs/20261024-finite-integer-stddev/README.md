# H043: prove integer standard-deviation input finite once

Production source: f936285fe1b3ac09848950bfd38aa4b0cc34b7e3, baseline main 0eec407731c5b39829316f8edf3f463e544b6888. One production Rust file changed. Native wheel origins, source hashes and differing native hashes are in provenance.json. Normal release wheels were built and installed, without binary copying, in separate environments on the same source path/target. Initial baseline build ~6 min, baseline integration-test graph 1m46s; corrected candidate warm build ~2 min and new Rust tests 24.91s. The initial candidate compile API error is preserved; its sole fix uses DF55 Cast.field.data_type().

## Change and scope

Standard DataFusion aggregate simplification proves Int32/Int64-to-Float64 casts finite using the schema. It retains the visible cast and every aggregate parameter so standard CSE/projection sharing still applies. Aggregate factory selects const FINITE ordinary/grouped adapters once. They skip the nonfinite scan/mask and delegate unchanged DataFusion population variance math. Raw typed integer UDAFs use the same strategy. Floating input, nonfinite merge flags, partial-state schema, filter handling, nulls, empty NaN, retraction and emission remain unchanged. Existing integer widening is retained; this does not claim a vectorized Welford kernel, elimination of casts, or memory-bandwidth saturation. Row fallback with erased cast lineage remains conservative.

No new per-entry loop, branch, dispatch, allocation or hand-written SIMD was introduced. Existing Arrow casts and DF accumulator math are reused. Grouped convert_to_state retains the required zero flag-state allocation; avoiding it would change the protocol.

## Public correctness

Baseline: 3 new Rust tests and 138 Python tests passed. Candidate: those same 141 cases plus 4 existing population-standard-deviation Rust tests passed (145 total). Independent Python statistics oracles cover int32/int64 extrema, float32/64, nullable and sliced arrays, empty/all-null, sparse matrix global/row reductions, filtering, keyed groups, persisted native roundtrip, full stats/shared SUM. Rust public aggregate APIs cover partial-state merge, retraction, grouped conversion, filters and partial emission. No mocks or plan-topology assertions. All raw gate logs saved.

## Native throughput and resource evidence

AMD EPYC 7B13 n2d-standard-32 / 128GB PRIMARY VM. Native fixture: 500M nullable Int32 values, x=idx%1000, null every seventh row, 32 groups, 16 partitions, 1,003,006,272 compressed bytes. Same public native input in both normally installed environments. No build or other timed workload overlapped measurements. Four alternating process pairs per workload; first pair excluded because a brief evidence-transfer command began and was immediately cancelled. Within retained three pairs, first query warms each fresh process; nine subsequent samples form the reported medians. Raw receipts include every sample, including excluded data.

| workload | baseline wall | candidate wall | speedup | baseline CPU | candidate CPU |
|---|---:|---:|---:|---:|---:|
| selected integer stdev | 408.4 ms | 367.1 ms | 1.11x | 12.04 s | 10.80 s |
| grouped integer stdev | 507.5 ms | 474.7 ms | 1.07x | 14.52 s | 13.56 s |

First nonalternating run was smaller (1.05x/1.07x); this is a modest pass-elimination improvement. Full stats control 491.2→481.8 ms / 14.60→14.34 core-s, floating-expression control 411.9→396.9 ms / 12.08→11.70 core-s (single-process controls, not evidence of a float-specific optimization). RSS paired global 332180→331676 KiB, grouped389524→388104 KiB; no memory-reduction claim.

CPU-clock profiles have zero lost samples: baseline nonfinite_count 9.80% self, candidate no samples in that function. Baseline DF VarianceAccumulator update22.55% self; native Welford remains an important remaining cost. Profiles include startup and 24 repeated full queries, so percentages describe whole processes rather than isolated arithmetic. Unchanged widening/allocation/page-fault costs remain visible. Hardware cycles/instructions/cache-misses were explicitly attempted but VM PMU reports unsupported; no ALU/SIMD/cache or DRAM-bandwidth claim. Software counters include12 queries+startup: task-clock145.40→134.26core-s, wall5.25→4.90s, page faults9.78M→10.01M (no allocation/fault improvement claim).

## Reproduction and raw artifacts

measure.py prepares/runs the public fixture; controls.py runs full stats and floating expressions. run-cloud.sh/resume-cloud.sh document normal build/test/installation commands, profile-cloud.sh records real perf software counters and CPU samples. CARGO_BUILD_JOBS=24, CARGO_INCREMENTAL=0, RUSTFLAGS=-C target-cpu=native; kache normal wrapper. All text logs and compressed full perf reports are committed. Local early50M profile merely established target reachability; final500M cloud evidence determines conclusions.

Large raw profiles are retained outside Git at /private/tmp/h043-raw-profiles.tar.gz and on the stopped VM /home/danielking/finite-stddev-20261024/raw-profiles.tar.gz. Archive SHA256 0e8c08fb6b745d9fa39faa97d923c6f3b2fedfdd5520aed87f6bab07d6342962; base.perf.data SHA25685da562eb67d919ca0bb5b604dee369d38ef4a90402e537bee521e41d0db2699, candidate.perf.data SHA2566e69f6a54c5739820d9701972ca97724052186afbd0f1d0631ce4f283f0fbc41. Small evidence archive downloaded and checksum verified6fa381e4e893db9b6b2ebe76386a50577ec333fc70108cc3e856a62c03b5a11c. Four-hour shutdown deadline installed before work; VM stopped after verified downloads.
