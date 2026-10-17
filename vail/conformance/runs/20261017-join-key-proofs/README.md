# Join key proofs: normal-policy cloud validation

Exact upstream cases, full inputs (`smoke_fixtures=false`), run serially on the existing 32-vCPU / 128-GiB GCP VM with 10 Hz CPU/RAM/I/O telemetry. Reuse the unchanged Hail baseline from the same VM. No global one-row setting, debug instrumentation, or singleton-prefetch experiment is included in these measurements.

Production source is main `c510e35dbfb9ee1bfeac7117b7843bdf3b77e30a` plus the key-proof/coalesce changes in `8341d3c6a366dd55a0e0e472a94ccbcb24679c60`. SHA-256 verification matched all five production files changed since original benchmark source `2a0c62741209f14e83f689e0d4609c50a0dc2986`: advanced_numeric_aggregates.rs, join_interval.rs, joins.rs, lib.rs and logical_aggregation.rs. The raw runner output is adjacent. Subsequent commits change tests only.

| Case | Original Vail s | Updated Vail s | Hail s | Updated Vail core-s | Updated peak GiB |
|---|---:|---:|---:|---:|---:|
| Wide inner join | 39.460 | 9.946 | 3.645 | 14.24 | 11.386 |
| Wide left join | 37.842 | 9.551 | 3.848 | 15.38 | 10.710 |
| Wide map/filter | 6.156 | 5.671 | 3.572 | 9.59 | 5.992 |
| Partition join 10 / 1000 | 10.060 | 2.738 | 7.083 | 34.15 | 0.727 |
| Partition join 100 / 1000 | 8.784 | 2.365 | 5.488 | 22.06 | 0.752 |
| Partition join 1000 / 10 | 9.148 | 2.862 | 7.202 | 35.22 | 0.752 |
| Partition join 1000 / 100 | 8.188 | 2.381 | 5.293 | 22.34 | 0.771 |
| Partition join 1000 / 1000 | 8.463 | 2.468 | 5.067 | 20.81 | 0.733 |
| Worst-case downsample, accounting fix baseline | 12.089 | 3.018 | 4.669 | 20.83 | 0.316 |

The region-memory cases still trail Hail and retain large buffers. Work continues on repeated closed-expression materialization and batch sizing. The partition sweep and parallel downsample are separately reviewed changes already on main. These observations are single query runs, not confidence intervals.

Root key-proof changes remove no-op naive coalesces, preserve unique nonnull Range-key proofs through key-forwarding row maps and order-preserving subsets, and omit the final ordinal sort only for Inner/Left joins with both inputs proven unique and a single join key. Other cases retain their existing sort. Public tests cover rows/order/value columns, unmatched and empty-right left joins, coalesce partition limits and invalid zero. The original focused test pass preceded a formatting step that accidentally inserted a filename into the test file; review caught that artifact before integration, it was removed, and the entire exact repaired 13-test IR file passed afterward. No performance run used the malformed test file.
