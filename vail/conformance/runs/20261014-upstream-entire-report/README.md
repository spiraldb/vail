# Entire upstream Hail Python API suite

Tested implementation: `974da796eddda33ebd0736d1f207bbfc23c1b6cc` on
`dk/randomness`, based on the requested
`36900efc8fd44a0adc8ee3efb03bae6a2c4b694c`.
Pinned upstream Hail: `e10cc355bc8cd0de9c5f29d5182d31527898bae1`.

**All upstream tests using randomness do not pass.**

All 62 `test_*.py` modules under `hail/hail/python/test/hail` were attempted,
including backend-specific modules. This covers the upstream Hail Python API
suite, not Hail's Scala compiler tests or the separate hailtop suite. The
source census has 1,546 test definitions; parametrization produces more cases.

## Authoritative pytest attempt

- 1,620 collected cases.
- 399 passing test calls; 702 failing test calls.
- 453 setup errors; 27 collection errors.
- 48 skipped test calls; 8 skips during setup. Expected failures reported by
  pytest as skipped are included here, not counted as passes.
- One external timeout: `extract_intervals/test_locus_position.py::test_mt_lt`.
  The test process was killed after the declared 60-second limit plus a
  5-second allowance. Later cases in that module did not execute.

Counts distinguish test calls from setup and collection failures. They do not
count uncollected tests as passing. Source hashes and all module outcomes are
in [modules.json](../20261014-upstream-entire-pytest/modules.json); per-module
logs and pytest event streams are stored alongside it.

The test runner redirects Hail imports to Vail in a temporary source copy,
omits Hail initialization/JVM health fixtures, adapts backend-flag decorators
and sample snapshot loading, and preserves test bodies and assertions. In
particular, it does not replace or bypass Hail IR assertions. This is an
adapted upstream run against Vail, not an unmodified Hail-runtime run.

## Randomness outcomes

The six tests in upstream `test_randomness.py` all pass under pytest.
The broader 131-definition selection used in the preceding audit has:

- 20 definitions whose observed pytest test calls pass.
- 56 definitions with failures (body or setup).
- 2 skipped/incomplete definitions.
- 53 definitions that could not execute through pytest.

The selection finds direct `rand_*`, `shuffle`, and sampling calls plus
same-module callers. It does not guarantee discovery of every indirect
randomness consumer. Nevertheless, the failures decisively disprove that all
randomness consumers pass. Exact cases and errors are in
[randomness-results.json](randomness-results.json).

Concrete failures include:

- `Tests.test_sample_rows`: MatrixTable sampling is still a stub.
- `test_sample_entries`: `Table.sample` is missing.
- `Tests.test_agg_array_agg_sum_vs_sum` and `Tests.test_agg_corr`: aggregate
  planning rejects expressions with `aggregation expressions require Table.aggregate`.
- Matrix structural randomness tests: access to `vail.ir.MatrixRead` and the
  other expected Hail IR types raises `NotImplementedError`.

## Supplemental body attempts

Every synchronous test definition is also attempted in a separate process by
`full_hail_bodies.py`, including Table, MatrixTable and backend definitions.
The single asynchronous filesystem definition is attempted separately through
the same worker. These attempts retain original assertions but adapt imports,
initialization, decorators and fixture setup more extensively than pytest.
They are supplemental evidence, not additional pytest passes, and cannot be
added to the pytest counts. A worker has a 45-second limit per definition;
worker errors, missing fixtures and timeouts are not passes.

Final supplemental counts are in [summary.json](summary.json), and individual
results are in
[results-parallel.json](../20261014-upstream-entire-bodies/results-parallel.json)
and [async-fs.json](../20261014-upstream-entire-bodies/async-fs.json).

All 1,546 definitions were attempted: 516 passed, 950 had failed or blocked
variants, 3 were blocked during setup/parameter evaluation, and 77 had worker
failures or timeouts. The randomness selection has 24 passing definitions,
99 with failed/blocked variants, and 8 worker failures. These are definition
counts, not parametrized-case counts.

## Reproduction

From `vail/`, with its Python dependencies and the current extension available:

```sh
PYTHONPATH=/absolute/path/to/vail/vail/python .venv/bin/python \
  conformance/full_hail_pytest.py \
  --source ../hail/hail/python/test/hail --include-backend \
  --module-timeout 600 --per-test-timeout 60 --output /tmp/upstream-pytest

PYTHONPATH=/absolute/path/to/vail/vail/python .venv/bin/python \
  conformance/full_hail_bodies.py \
  --suite ../hail/hail/python/test/hail --include-backend --include-relational \
  --jobs 4 --output /tmp/upstream-bodies
```

The supplemental enumerator currently handles synchronous definitions. For the
one asynchronous definition, use the worker with `--worker-source
../hail/hail/python/test/hail/fs/test_worker_driver_fs.py --worker-name
test_qob_can_use_sas_tokens --suite ../hail/hail/python/test/hail --output
/tmp/upstream-async-fs.json`.

No production implementation changes were made during these runs. Runner
options were added to include backend and relational definitions that the
existing tools excluded by default.
