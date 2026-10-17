# Dense downsample profile (2026-10-16)

This run isolates the exact upstream `test_table_aggregate_downsample_dense` benchmark. The benchmark function was not edited. The benchmark runner loaded that function from the Hail source tree, reused the same cached `many_ints_ht` fixture for the after run, and timed only the query. The upstream `benchmark_table.py` SHA-256 was `a5e0c64d261d174743551150d6952abcb18fa615abe86950f673cb32d1d3b22d`; the source checkout was Hail `e10cc355bc8cd0de9c5f29d5182d31527898bae1`.

## Results

| Run | Query wall time | Worker CPU time | Average cores | Peak RSS |
| --- | ---: | ---: | ---: | ---: |
| Before | 53.788 s | 45.105 s | 0.839 | 1.264 GiB |
| After | 3.814 s | 4.198 s | 1.101 | 1.159 GiB |

The after run is 14.1x faster. Both use 10 Hz process telemetry; these are local laptop measurements, not the separate GCP paired benchmark. Fixture preparation is excluded from query wall time. The before fixture took 205.416 s to materialize; the after run reused it (0.075 s fixture setup).

## Profile finding

The before `/usr/bin/sample` captured 795 stack samples over 10 seconds. `DownsampleAccumulator::size` accounted for 640 frames across its two hot call sites (about 81% of samples); `ScalarValue::size` appeared in 579 frames. The repeated retained-point/label traversal dominated aggregation. After the fix, a 400-sample 10-second profile had no `DownsampleAccumulator::size` or `ScalarValue::size` frames; the largest remaining aggregate frames were `update_batch` (208) and `add_rows` (197).

Telemetry CSVs are worker-process-only 10 Hz samples, including CPU/RSS and process counters; system process enumeration was unavailable in this sandbox. Stack profiles sample the benchmark worker. The JSON records each runner/source digest and per-run timing/counter data. The before and after native extensions were built from this candidate source; the after extension SHA-256 was `047ecbb3c2fe1c78e87bd9126fb778efb08b46740bb7ddc8c88b50b2f44fc962`.

## Artifacts

- `before.json`, `after.json`: benchmark runner results and input digests.
- `before.metrics.csv`, `after.metrics.csv`: 10 Hz worker telemetry.
- `before.sample.txt`, `after.sample.txt`: macOS stack samples.

The source revision before changes was `2a0c62741209f14e83f689e0d4609c50a0dc2986`. The candidate changed only the downsample accumulator and its public Python regression test. Full suite: 1855 passed, 1 skipped, 19 subtests passed (85.24 s), with Hail source checked out at the revision above.
