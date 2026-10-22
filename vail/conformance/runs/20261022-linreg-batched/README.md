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
