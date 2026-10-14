# Randomness verification

Base: `36900efc8fd44a0adc8ee3efb03bae6a2c4b694c`.
Branch: `dk/randomness`.

- Randomness: **123 passed**, including 94 Hail behavioral bodies and checks
  for all 13 public `hl.rand_...` functions and their signatures.
- Native generator/replay checks: **3 passed**.
- Wider Python/harness run: **1,190 passed, 3 failed, 1 skipped**, plus
  19 passing subtests. All three failures reproduce on a clean build of the
  exact base commit; they test pruning after distant source files are deleted.

The existing failures are:

- `test_interval_partition_join_reads_only_files_needed_for_selected_output`
- `test_membership_join_executes_only_selected_left_partition[semi_join-expected0]`
- `test_membership_join_executes_only_selected_left_partition[anti_join-expected1]`

The baseline build was made from a `git archive` of the base, with its own
original Python frontend and native extension. See the saved baseline log.
The focused suite was rerun after the final stream-prefix correctness check.
Hail compiler-node assertions are adapted to Vail IR; behavioral assertions
are retained. See [the design](../../../docs/RANDOMNESS.md).

Commands, from `vail/` with the native extension built:

```sh
PYTHONPATH=python .venv/bin/python -m pytest python/tests/test_randomness.py -q
PYTHONPATH=python .venv/bin/python -m pytest python/tests conformance/test_harness.py -q
cargo test --workspace --lib --release --locked randomness::tests
cargo test --workspace --lib --release --locked interleaved_partition_replays_keep_independent_state
```

The summary includes the tested native extension's SHA-256. The source manifest
records the final implementation/test files, including the stream-prefix
strengthening verified by the final focused rerun.
