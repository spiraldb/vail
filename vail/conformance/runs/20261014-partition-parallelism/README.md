# Partition concurrency verification

Verified the 32-way partition execution changes using a separate debug wheel
and Python environment at `/private/tmp/vail-parallel-tests`. The optimized
benchmark environment was left unchanged. No benchmarks or telemetry were run.

- Complete local Python suite: **1,031 passed, 1 skipped, 19 subtests passed**.
- Rust `vail-table` library tests: **31 passed, 0 failed**.
- Focused Matrix grouping/coordinate conversion regressions: **4 passed**.

The first complete run found eight failures. Two asserted the former single-stage
aggregate plan rather than DataFusion's new partial/final plan. The others exposed
empty physical sources and adapters that consumed only child partition zero
after optimization introduced additional partitions. These were fixed before
the final full run. An earlier focused run also caught an ordering boundary that
overwrote an appended grouping column; that boundary now precedes scope columns.

New Rust tests explicitly verify concurrent producers, a 32-stream bound,
ordered consumption, cancellation, error propagation, and singleton-adapter
distribution requirements after child rewrites. New Python tests verify native
partial/final reductions, order-sensitive collections, partition counts, replay
after errors, empty inputs, and Matrix entry aggregation.

Commands, run from `vail/`:

```sh
/private/tmp/vail-parallel-tests/bin/python -m pytest python/tests -q \
  --junitxml=conformance/runs/20261014-partition-parallelism/python-final.xml
RUSTC_WRAPPER='' CARGO_INCREMENTAL=0 \
  CARGO_TARGET_DIR=/private/tmp/vail-native-types-target \
  cargo test -p vail-table --lib
```

Full logs and XML remain locally in this directory. Only this summary is committed.
