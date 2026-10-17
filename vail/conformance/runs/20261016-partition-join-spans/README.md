# Partitioned inner-join span fast path

The upstream `test_join_partitions_table` query-only timings below use the exact 10M-row fixtures and `smoke_fixtures=false`; fixture construction is reported separately and excluded from `benchmark_seconds`. The runner uses upstream Hail benchmark source SHA-256 `a5e0c64d261d174743551150d6952abcb18fa615abe86950f673cb32d1d3b22d` and runner SHA-256 `a3eea43eda8e599c49026e4fb385f0fb1c3d30e9da8fc82e3d241e4b7b1d7a59`.

| Partitions | Before Vail (s) | After Vail (s) | Hail cloud reference (s) |
|---|---:|---:|---:|
| 10 × 1000 | 3.41 | 1.542 | 7.083 |
| 100 × 1000 | — | 1.264 | 5.488 |
| 1000 × 10 | — | 1.538 | 7.202 |
| 1000 × 100 | — | 1.305 | 5.292 |
| 1000 × 1000 | 3.407 | 1.331 | 5.067 |

Before timings were measured on the same laptop with the same exact query and fixtures before the span fast path. The before native artifact SHA-256 was `047ecbb3c2fe1c78e87bd9126fb778efb08b46740bb7ddc8c88b50b2f44fc962`; it was the pre-serialized-aggregation-whitelist package. The 2-second profile capture used native artifact SHA-256 `10175e6cc9f8629323c2c4bc27331f067576fb2704281bc246660750b88bbd45`, which includes the subsequent downsample-only serialized-aggregation whitelist change. That change affects aggregation classification and is unrelated to joins. After timings use source based on `f032465d3a3d25e8ec3b49306188bd6b5b92d2b2` plus this change and have native artifact SHA-256 `2f0e83467ec244dc5492ff37344dfb949903f4eb1b9ce44a42c9075a546bdd68`.

The 100 × 1000, 1000 × 10, and 1000 × 100 rows have no same-laptop before measurements; their Hail references are historical GCP measurements, not paired local runs. All cloud numbers are reference only. The local before/after measurements establish the 1000 × 1000 and 10 × 1000 speedups; Hail parity on cloud must be remeasured after this change before claiming a cloud win.

A 2-second macOS `sample` capture of the 1000 × 1000 query before the change recorded the main thread in interval preparation/planning in 88/121 samples, including 64 samples in DataFusion physical planning. The implementation replaces global endpoint-cell fanout for certified strictly disjoint inner-join inputs with a sorted two-pointer overlap sweep and one inclusive clipped join per intersecting partition pair. It retains the existing cell planner for outer/semi/anti joins, non-disjoint inputs, empty/single-overlap ambiguity, and large raw `idx` range inputs that require bounded streaming subdivision. Null keys remain included in bound ordering and use the existing `NullEqualsNothing` join semantics.

Final validation: Vail Python suite `1857 passed, 1 skipped, 19 subtests passed`; exact upstream partition join cases all passed. See `after-local.json` for per-case timings and fixture durations.

Required review: chat `01a14128-21b8-7a53-b399-4ad8f9b31f4f` approved exact candidate `e56c731c44cfaea6a35417e94609598bc2819680` against `f76d4a5ebbd8cb88016b8b08ea12dd821f7318db`. The source and tests are unchanged from reviewed `2324544a03f946fbb624a1ec5d3a7c0db58340ba`; the child corrects artifact provenance. The reviewer cleared disjoint-bound eligibility, inclusive clipping, duplicate products, null/empty behavior and the large-range streaming fallback. Enumeration and plan count are linear in input partition counts; execution may reread a broad partition for multiple peers. Cloud validation remains pending. Reviewer performed no builds, tests or edits. No blocking findings remain for this candidate; other join/batching experiments are outside this approval.
