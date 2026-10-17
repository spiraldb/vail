# Worst-case downsample profile (2026-10-16)

This run uses the unchanged upstream `test_table_aggregate_downsample_worst_case`: `hl.utils.range_table(250_000_000, 8)` followed by `hl.agg.downsample(ht.idx, -ht.idx)`. The upstream source SHA-256 is `a5e0c64d261d174743551150d6952abcb18fa615abe86950f673cb32d1d3b22d` (Hail checkout `e10cc355bc8cd0de9c5f29d5182d31527898bae1`). Both runs used the same temporary runner SHA-256, `07396db3243044656b97abc839a0fc4585312b5f36d285a3bd9e30cf07f966ad`. The runner points `UPSTREAM` to that Hail checkout, writes the spawned worker PID to a temporary marker, and waits at a temporary trigger so `sample` can attach before timing begins. These adapter changes do not modify the benchmark function or its timed query region.

## Results

| Run | Query wall time | Worker CPU time | Average cores | Peak RSS |
| --- | ---: | ---: | ---: | ---: |
| Before partial aggregation | 8.195 s | 7.625 s | 0.930 | 0.130 GiB |
| After partial aggregation | 1.031 s | 7.084 s | 6.873 | 0.216 GiB |

The local query is 7.95x faster after allowing DataFusion to use partial states and merge them. Worker CPU time stayed close while average core use rose from under one to about 6.9. Both are local laptop measurements of the exact query; they are not the separate GCP paired benchmark. Query time excludes runner setup (there are no upstream fixtures for this case). Disk counters were 90.4 MB read before and zero after; because this is a range-table workload and the after run was warm, use the wall/CPU figures as the primary comparison.

The before native extension SHA-256 was `047ecbb3c2fe1c78e87bd9126fb778efb08b46740bb7ddc8c88b50b2f44fc962`, built with the prior downsample accounting fix. The after binary SHA-256 is `10175e6cc9f8629323c2c4bc27331f067576fb2704281bc246660750b88bbd45`, built from Vail revision `f032465d3a3d25e8ec3b49306188bd6b5b92d2b2` plus this change. Both used the shared Python 3.12 environment. The release build used cargo/rustc 1.98.0 and the shared target directory.

## Profile finding

The before 10-second macOS sample (captured on the worker during the query) showed `DownsampleAccumulator::update_batch` in 94 main-thread stack samples, including 80 in `add_rows`; the query was effectively serialized. The after sample contained downsample frames on 23 Tokio runtime workers, including 60 `update_batch` and 56 `add_rows` symbol samples, consistent with the telemetry's 6.87 average cores. The operator's per-row loop and rebin policy were not changed; the improvement comes from parallel partial aggregation and the existing `merge_batch` implementation.

Hail's aggregator also combines per-partition states (`DownsampleAggregator._combOp`). Hail buffers up to 256 input points before rebinning; Vail retains its existing incremental bound-expansion/rebin policy. Downsample is documented as approximate and Hail does not promise stable representative identities across different partitionings. The public test therefore checks that outputs are finite input representatives with the corresponding labels, stay within the grid bound, occupy at most one output per final cell, and ignore missing/nonfinite coordinates; it does not pin unstable representative identity.

Telemetry was sampled at 10 Hz from the benchmark worker process only. Process-tree enumeration was unavailable, so descendants are not included; the JSON `cpu_scope` metadata reflects that. Stack profiles sample the worker PID. Full Python suite: 1855 passed, 1 skipped, 19 subtests passed (48.99 s), against Hail `e10cc355`.

## Artifacts

- `before.json`, `after.json`: timed result, telemetry deltas, runner and upstream source hashes.
- `before.metrics.csv`, `after.metrics.csv`: 10 Hz worker CPU/RSS/counter samples.
- `before.sample.txt`, `after.sample.txt`: macOS stack samples.

## Required review

Reviewer chat `01a14128-21b8-7a53-b399-4ad8f9b31f4f` approved exact `19dd184c90550455536a02419a09c6ccf42d5cc2` against main `f032465d3a3d25e8ec3b49306188bd6b5b92d2b2`. Production commit `e464af50` is unchanged by the final assertion-only child. All review findings were addressed. The reviewer inspected source/tests/evidence, ran no builds/tests, and made no edits. Local throughput improvement is not a cloud speedup claim; same-VM validation follows separately after the join diagnostic policy is restored.
