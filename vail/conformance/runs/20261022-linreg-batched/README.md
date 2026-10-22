# Linear regression batching baseline

Baseline source: `7e62dfbdadcc29af62d2581b5942f84b4d395e32` (`main-integration`, latest main checked at run start).

Local fixture: `/Users/danielking/projects/vail/vail/local-data/upstream-benchmark-20261015/vail/many_ints_ht.ht`.
Benchmark case: Hail upstream `test_table_aggregate_linreg` from `benchmark_table.py`, 75 fits over the four existing predictor columns.

Baseline wheel was built from the isolated checkout with Rust 1.98 and `maturin build --release --locked`, using `CARGO_TARGET_DIR=/Users/danielking/projects/vail/vail/target`. Wheel SHA256: `934107f751f13f90818a11c432ae8e9b43b8ad84f825581b9c7b92ed3bf77bdb`.

Three serial runs passed: 4.770, 4.821, and 4.757 seconds benchmark wall time (mean 4.783 seconds). Process CPU user time was 57.79, 57.87, and 58.38 seconds (mean 58.01 seconds). Logs and per-run JSON are alongside this file. A single sampled profile of the same current-main wheel passed in 6.108 seconds; the compressed raw profile and symbol sidecar are preserved here.

Each run used the same local benchmark driver and fixture. No Hail benchmark was rerun.

## Direct grouped-workspace candidate

Candidate source after numerical review fix: `31ac0397f4c7b96d86852af826b63cd61569097c` (`dk/linreg-batched`). It gathers selected rows in input order directly into a reusable, contiguous column-major workspace partitioned by group, then runs Faer Householder QR in place on each group view. The retained workspace is sized to the current Arrow batch. The grouped path also reuses predictor QR only when the full predictor matrix matches bit-for-bit; the response transform operates in its workspace column. Extreme mixed-magnitude columns retain the stable Givens fallback.

The candidate wheel was built with Rust 1.98.0, release mode, locked dependencies, and the project `.venv` CPython 3.12 interpreter. Final wheel SHA256: `d678a789e454b3438dc426a3636ad18b3748f18bea93dfd3998f802592051933`.

After the review fix, three serial runs of the same 75-fit upstream case passed: benchmark wall times 3.482, 3.561, and 3.480 seconds (mean 3.508, median 3.482). Process user CPU was 39.56, 39.25, and 39.43 seconds; system CPU was 1.97, 1.87, and 1.77 seconds (mean total 41.28 CPU seconds). Against the current-main three-run means (4.783 seconds benchmark wall and 59.67 total CPU seconds), this is 26.7% lower wall time and 30.8% lower CPU time. `/usr/bin/time -lp` peak resident set was 4.81–4.99 GB for the candidate and 3.99–4.25 GB for baseline; these process-level peaks varied across runs and are higher for the candidate, so the speed improvement comes with a memory peak that merits follow-up on other batch sizes.

A separate scalar one-fit check on the same 7.5M-row table had final candidate benchmark wall times 0.243, 0.253, and 0.286 seconds (median 0.253) versus baseline 0.447, 0.264, and 0.267 seconds (median 0.267; the first baseline run was a cold outlier). Candidate median user CPU was 0.82 seconds versus 1.01 seconds for baseline. This check found no singleton regression.

`vail/python/tests/test_agg_numeric.py` passed all 56 tests on the final candidate wheel, including weighted/null/rank/conditioning cases, extreme response scales, a grouped same-X cache/mask test, and a tiny residual case exercising cached Qᵀ. Candidate timing logs are `robust-residual-{1,2,3}.log`; final scalar logs are `robust-single-{1,2,3}.log`, with baseline logs `single-baseline*.log`. The required reviewer cleared source/correctness on exact revision `31ac0397f4c7b96d86852af826b63cd61569097c` in Codex thread `01a14128-21b8-7a53-b399-4ad8f9b31f4f`.


A final `samply` profile was captured on the exact final wheel/source at 1 kHz; the benchmark completed in 3.596s under sampling. Raw profile, symbol sidecar, and command output are `final-31ac0397-profile.json.gz`, `final-31ac0397-profile.json.syms.json`, and `final-31ac0397-profile-run.log`. The profile was captured after the three unprofiled timing runs.

For a repeat run, build the locked release wheel with Rust 1.98 using the documented maturin command, extract it to a site directory, and run:

```sh
VAIL_BENCHMARK_UPSTREAM=/Users/danielking/projects/vail/hail/hail/python/benchmark/hail \
PYTHONPATH=/tmp/linreg-final-site \
/usr/bin/time -lp /Users/danielking/projects/vail/vail/.venv/bin/python \
vail/conformance/runs/20261022-linreg-batched/run_upstream_linreg.py
```

`run_upstream_linreg.py` invokes the exact upstream `test_table_aggregate_linreg` case and existing local `many_ints_ht` fixture; it does not download fixture data.

## Position-based array aggregation candidate

Source revision: `eb048cca5f096fe7dc1363ebe0d7951b031caa0c` (after the direct-workspace QR change above). This extends the existing coordinate-group adapter to direct `agg.array_agg(... agg.linreg(...))` bodies. The plan uses `vail_coordinate_linreg` and has no `Unnest`; it avoids materializing an expanded relation and hash-grouping by position. The callback still builds mapped observation structs, so captured row columns are still broadcast into the per-position arrays. Filtered callbacks keep the prior relational fallback. The `MatrixGroupedTable.result(axis='row')` callback-struct shape also does not match this direct-aggregate fast path.

The exact candidate release wheel was built with Rust 1.98.0, locked release dependencies, and the project CPython 3.12 interpreter. Wheel SHA256: `069cc8672702505885a76cb452823d23a633d4e8e69de4078b9e9a12abc98b8a`. `test_agg_numeric.py` passed all 57 tests, and six focused aggregation/composition/nested-aggregation tests passed. The plan check used `range_table(8)` and two coordinate linregs, and verified `Unnest` was absent.

Three serial runs of upstream `test_table_aggregate_linreg` against the local 7.5M-row `many_ints_ht` fixture passed. Query wall times were 3.663, 3.640, and 4.844 seconds (median 3.663); worker plus live-descendant CPU was 33.967, 35.649, and 36.738 seconds (median 35.649); sampled peak RSS was 2.449, 2.495, and 2.469 GiB (median 2.469). The third wall time is an outlier. Compared with the matching current-main query-scope baseline (median 3.378 seconds wall, 38.540 CPU seconds, and 4.303 GiB peak RSS), the candidate is about 8% slower by median wall time, 7.5% lower in CPU time, and 42.6% lower in sampled peak RSS. This change removes unnest/hash grouping and lowers CPU and memory for this case; it does not improve median wall time.

The baseline is source `31ac0397f4c7b96d86852af826b63cd61569097c`, wheel SHA256 `d678a789e454b3438dc426a3636ad18b3748f18bea93dfd3998f802592051933`; its three query-scope JSONs are copied here as `baseline-vail-matched-run-{1,2,3}.json`. It used the same runner SHA256 `2b9d1bd42cb75c1087f1b90c34a14426fdcd8284a96fdd50580e4cc279bb7044` and same fixture. No known unrelated build or benchmark overlapped the final candidate timing loop; the earlier fixture setup attempt was aborted before it emitted `status=ready` and before starting the query timer. Candidate logs are `coordinate-arrayagg-{1,2,3}.log`; 10 Hz per-run process-tree telemetry appears in each log, with the last sampled summary in `coordinate-telemetry/`.

For repeatability, the exact run command was:

```sh
VAIL_BENCHMARK_TELEMETRY_DIR=/Users/danielking/.codex/worktrees/linreg-batched/vail/vail/conformance/runs/20261022-linreg-batched/coordinate-telemetry \
VAIL_BENCHMARK_UPSTREAM=/Users/danielking/projects/vail/hail/hail/python/benchmark/hail \
PYTHONPATH=/tmp/linreg-coordinate-site:/Users/danielking/.codex/worktrees/linreg-batched/vail/vail/python \
/usr/bin/time -lp /Users/danielking/projects/vail/vail/.venv/bin/python \
vail/conformance/runs/20261022-linreg-batched/run_upstream_linreg.py
```

The worker reused `/Users/danielking/projects/vail/vail/local-data/upstream-benchmark-20261015` via a temporary symlink at `vail/local-data/upstream-benchmark-20261015` in this isolated worktree; fixture preparation is outside the query timing. `coordinate-arrayagg-1.log` has a one-time 513 MB disk-read counter, while runs 2 and 3 report zero disk reads.
