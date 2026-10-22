# Linear regression batching baseline

Baseline source: `7e62dfbdadcc29af62d2581b5942f84b4d395e32` (`main-integration`, latest main checked at run start).

Local fixture: `/Users/danielking/projects/vail/vail/local-data/upstream-benchmark-20261015/vail/many_ints_ht.ht`.
Benchmark case: Hail upstream `test_table_aggregate_linreg` from `benchmark_table.py`, 75 fits over the four existing predictor columns.

Baseline wheel was built from the isolated checkout with Rust 1.98 and `maturin build --release --locked`, using `CARGO_TARGET_DIR=/Users/danielking/projects/vail/vail/target`. Wheel SHA256: `934107f751f13f90818a11c432ae8e9b43b8ad84f825581b9c7b92ed3bf77bdb`.

Three serial runs passed: 4.770, 4.821, and 4.757 seconds benchmark wall time (mean 4.783 seconds). Process CPU user time was 57.79, 57.87, and 58.38 seconds (mean 58.01 seconds). Logs and per-run JSON are alongside this file. A single sampled profile of the same current-main wheel passed in 6.108 seconds; the compressed raw profile and symbol sidecar are preserved here.

Each run used the same local benchmark driver and fixture. No Hail benchmark was rerun.
