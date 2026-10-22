# Linear regression batching baseline

Baseline source: `7e62dfbdadcc29af62d2581b5942f84b4d395e32` (`main-integration`, latest main checked at run start).

Local fixture: `/Users/danielking/projects/vail/vail/local-data/upstream-benchmark-20261015/vail/many_ints_ht.ht`.
Benchmark case: Hail upstream `test_table_aggregate_linreg` from `benchmark_table.py`, 75 fits over the four existing predictor columns.

Baseline wheel was built from the isolated checkout with Rust 1.98 and `maturin build --release --locked`, using `CARGO_TARGET_DIR=/Users/danielking/projects/vail/vail/target`. Wheel SHA256: `934107f751f13f90818a11c432ae8e9b43b8ad84f825581b9c7b92ed3bf77bdb`.

Three serial runs passed: 4.770, 4.821, and 4.757 seconds benchmark wall time (mean 4.783 seconds). Process CPU user time was 57.79, 57.87, and 58.38 seconds (mean 58.01 seconds). Logs and per-run JSON are alongside this file. A single sampled profile of the same current-main wheel passed in 6.108 seconds; the compressed raw profile and symbol sidecar are preserved here.

Each run used the same local benchmark driver and fixture. No Hail benchmark was rerun.

## Direct grouped-workspace candidate

Candidate source: `46dedd4bbc714171e61f7a873f403a4744bf99e3` (`dk/linreg-batched`). It gathers selected rows in input order directly into a reusable, contiguous column-major workspace partitioned by group, then runs Faer Householder QR in place on each group view. The retained workspace is sized to the current Arrow batch. The grouped path also reuses predictor QR only when the full predictor matrix matches bit-for-bit; the response transform operates in its workspace column. Extreme mixed-magnitude columns retain the stable Givens fallback.

The candidate wheel was built with Rust 1.98.0, release mode, locked dependencies, and the project `.venv` CPython 3.12 interpreter. Wheel SHA256: `464e15be948af8a5fb50e6b32ee868d1aad4aef0b561cced4baff81a4e5652d8`.

Three serial runs of the same 75-fit upstream case passed: benchmark wall times 3.549, 3.504, and 3.908 seconds (mean 3.654, median 3.549). Process user CPU was 42.11, 41.60, and 40.98 seconds; system CPU was 1.95, 2.04, and 1.58 seconds (mean total 43.42 CPU seconds). Against the current-main three-run means (4.783 seconds benchmark wall and 59.67 total CPU seconds), this is 23.6% lower wall time and 27.2% lower CPU time. `/usr/bin/time -lp` peak resident set was 4.76–5.05 GB for the candidate and 3.99–4.25 GB for baseline; these process-level peaks varied across runs and are higher for the candidate, so the speed improvement comes with a memory peak that merits follow-up on other batch sizes.

A separate scalar one-fit check on the same 7.5M-row table had candidate benchmark wall times 0.248, 0.253, and 0.252 seconds (median 0.252) versus baseline 0.447, 0.264, and 0.267 seconds (median 0.267; the first baseline run was a cold outlier). The candidate's median user CPU was 0.84 seconds versus 1.01 seconds for baseline. This check found no singleton regression.

`vail/python/tests/test_agg_numeric.py` passed all 55 tests on the candidate wheel, including weighted/null/rank/conditioning cases, extreme response scales, and the added grouped cache/mask test. Candidate timing logs are `direct-workspace-{1,2,3}.log`; scalar logs are `single-{baseline,candidate}*.log`. Review of the exact source revision is pending in Codex thread `01a14128-21b8-7a53-b399-4ad8f9b31f4f`.
