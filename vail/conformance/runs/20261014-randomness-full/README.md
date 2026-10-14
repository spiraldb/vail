# Full validation follow-up

Tested commit: `974da796eddda33ebd0736d1f207bbfc23c1b6cc`, on the requested
base `36900efc8fd44a0adc8ee3efb03bae6a2c4b694c`.

The complete Vail-owned suites do **not** pass:

- Rust workspace/all targets (release): 208 passed, 1 failed, 1 hung.
  `distinct_rejects_float_equivalence_mismatch` fails. The concurrency test
  `force_count_drains_later_partitions_while_first_is_gated` hung in the full
  run and was attempted separately with a 45-second process timeout. All other
  targets were run with `--no-fail-fast`, skipping only that hung test.
- Python and conformance harness: 1,191 passed, 3 failed, 1 skipped, plus
  19 passing subtests. The three file-pruning failures match the prior run
  and its clean-base reproductions.
- Native conformance: 4/4 passed. Upstream table cases: 103/103 passed.
- Test-runner self-tests: 9/9 passed.
- Hail oracle: 4 unavailable (`hailtop.version` missing); no oracle passes.

The upstream randomness audit attempted 131 definitions selected from calls to
`rand_*`, `shuffle`, `sample`, `sample_rows`, and `sample_cols`, plus same-module
callers. It found 24 passing definitions, 80 blocked on compiler IR/backend
access, 19 other failures/blockers, and 8 blocked workers. These are definitions,
not parametrized variant counts. This static selection is not a guarantee of
finding every indirect use, and this is not the entire Hail test suite.
Original assertions were retained in this audit; the existing body runner
adapts imports, initialization, decorators and fixtures.

Concrete missing randomness consumers include `Table.sample`,
`MatrixTable.sample_rows`, and `MatrixTable.sample_cols`. Tests also encounter
unimplemented statistical methods, file importers and experimental APIs.

## IR assertion limitation

The focused randomness suite's IR adapter is **not equivalent** to the original
Hail assertion coverage. Many table checks search actual Vail nodes (with
explicit mappings). Matrix checks only assert that the native core serializes;
several matrix-to-table/non-table checks similarly accept any nonempty IR.
These checks do not prove that the intended operation is present. Behavioral
uniqueness/value assertions still execute, but their passes must not be reported
as full upstream structural test passes.

Vail exposes native table/expression IR. Matrix operations are lowered directly
into a table relation with column metadata and entry arrays, without preserving
Hail's individual MatrixIR node types. Meaningful replacements require checking
the corresponding lowering structure/expressions and rejecting unchanged-input
negative controls. Generic nonempty-IR checks cannot establish that equivalence.

## Commands

From `vail/`, using the existing current native extension:

```sh
RUSTUP_TOOLCHAIN=1.98.0 RUSTC_WRAPPER='' \
  RUSTFLAGS='-C link-arg=-undefined -C link-arg=dynamic_lookup' \
  CARGO_TARGET_DIR=/private/tmp/vail-randomness-target \
  cargo test --workspace --all-targets --release --locked --no-fail-fast
PYTHONPATH=python .venv/bin/python -m pytest python/tests conformance/test_harness.py -q
PYTHONPATH=python .venv/bin/python scripts/run_all_tests.py --self-test
PYTHONPATH=python .venv/bin/python -m conformance.run --backend native \
  --command '.venv/bin/python -m scripts.native_conformance_worker'
PYTHONPATH=python .venv/bin/python -m conformance.upstream_table_cases
PYTHONPATH=python .venv/bin/python -m conformance.run --backend oracle
```

The first Rust attempt without `--no-fail-fast` stopped at the first failing
target. The full run with `--no-fail-fast` was interrupted when the concurrency
test hung; `full-rust-rest.log` records all remaining targets with that single
test skipped. No implementation source was changed during these runs.
