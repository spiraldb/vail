# Dense downsample profile (2026-10-16)

This run isolates the exact upstream `test_table_aggregate_downsample_dense` benchmark. The benchmark function was not edited. The benchmark runner loaded that function from the Hail source tree, reused the same cached `many_ints_ht` fixture for the after run, and timed only the query. The upstream `benchmark_table.py` SHA-256 was `a5e0c64d261d174743551150d6952abcb18fa615abe86950f673cb32d1d3b22d`; the source checkout was Hail `e10cc355bc8cd0de9c5f29d5182d31527898bae1`.

## Results

| Run | Query wall time | Worker CPU time | Average cores | Peak RSS |
| --- | ---: | ---: | ---: | ---: |
| Before | 53.788 s | 45.105 s | 0.839 | 1.264 GiB |
| After | 3.814 s | 4.198 s | 1.101 | 1.159 GiB |

The after run is 14.1x faster. Both use 10 Hz telemetry of the benchmark worker process; these are local laptop measurements, not the separate GCP paired benchmark. Fixture preparation is excluded from query wall time. The before fixture took 205.416 s to materialize; the after run reused it (0.075 s fixture setup).

The runner source SHA-256 was `3d4b200822f38bc280df797e3bc004502cb2ad88f877ca8d0bfb12ef27d28732` before and `a2d32b5fa23918f00430961c670c67a18cf6e0c768587de241f651156a6ac426` after. The only runner-source change was writing the spawned worker PID to a temporary file for process sampling; it did not change fixture handling, benchmark invocation, or timed region. Both runs discovered the same upstream benchmark function, whose source digest is recorded in the JSON files. The same cached fixture was used for the after run.

## Profile finding

The before `/usr/bin/sample` captured 795 stack samples over 10 seconds. `DownsampleAccumulator::size` accounted for 640 frames across its two hot call sites (about 81% of samples); `ScalarValue::size` appeared in 579 frames. The repeated retained-point/label traversal dominated aggregation. After the fix, a 400-sample 10-second profile had no `DownsampleAccumulator::size` or `ScalarValue::size` frames; the largest remaining aggregate frames were `update_batch` (208) and `add_rows` (197).

Telemetry CSVs are worker-process-only 10 Hz samples, including CPU/RSS and process counters. The telemetry helper snapshots only its worker PID because process-tree enumeration was unavailable in this sandbox; it does not include descendant processes. The JSON `cpu_scope` field originally claimed worker plus descendants, which was inaccurate; the archived JSON metadata has been corrected to say worker process only. Stack profiles sample the benchmark worker. The baseline extension SHA-256 was `1ddf47e534b32cc20d3f534a03a2ae07035dedbe531eddc0c1e363ef80d57cd9`, from the pre-change candidate package. The after extension was built from this change and has SHA-256 `047ecbb3c2fe1c78e87bd9126fb778efb08b46740bb7ddc8c88b50b2f44fc962`.

## Artifacts

- `before.json`, `after.json`: benchmark runner results and input digests.
- `before.metrics.csv`, `after.metrics.csv`: 10 Hz worker telemetry.
- `before.sample.txt`, `after.sample.txt`: macOS stack samples.

The source revision before changes was `2a0c62741209f14e83f689e0d4609c50a0dc2986`; the baseline binary hash above identifies the extension used for the before run. The candidate changed only the downsample accumulator and its public Python regression test. Full suite: 1855 passed, 1 skipped, 19 subtests passed (85.24 s), with Hail source checked out at the revision above.

## Required review

Reviewer chat `01a14128-21b8-7a53-b399-4ad8f9b31f4f` approved exact `d2806f0f562b3631de3d590752e7c21d1f4acff3` against main `2a0c62741209f14e83f689e0d4609c50a0dc2986`, including unchanged production/test commit `9f8320c047648011ada7912ac62ab1267d3e3460`. Source invariants, public regressions, full Python results and corrected profile provenance were reviewed; all findings were addressed. The reviewer ran no builds/tests and made no edits. Local timings describe this workload only; the same-VM cloud follow-up is separate. This child changes only the approval record.
