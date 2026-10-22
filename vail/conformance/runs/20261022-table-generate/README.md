# Table._generate

Source/test revision: `861320e7b99e2ee9d412c8019050bf928c61d191`.

The integer-partition TableGen path is implemented using the existing native
expression-array source, globals binding, projection and explode operations.
The Python callback builds one expression graph; it is not invoked per row or
partition. Runtime context count must equal partition count. Missing arrays and
mismatched counts fail at action time. Each context owns one input partition,
including partitions that emit no rows. Result fields are exactly the callback
struct fields; helper columns do not escape.

Contexts are materialized as one native array; each callback returns a native
array for its context. This is lazy execution, not a streaming generator or a
constant-memory guarantee.

Globals and contexts are validated as unindexed expressions. Shared globals,
random context/row expressions, distinct downstream row identities, repeated
actions, variable-length output and partition membership are covered publicly.
Hail references read: Python table.py `_generate`, IR TableGen randomness
handling, and Scala LowerTableIR.scala TableGen runtime lowering.

Positive integer counts and unkeyed interval lists are supported. Keyed interval
partitioners explicitly raise NotImplementedError: they require interval range
validation and a defensible key-order guarantee. No key metadata is fabricated.

Validation uses the existing approved native artifact; this Python-only change
requires no Rust changes or new native build. `settings.json` records exact
source/native provenance. Fresh processes import this checkout by absolute
PYTHONPATH.

- `focused.log`: 99 passed across generation (14 cases), expression inputs,
  partition-stream mapping and randomness.
- `upstream/`: all 36 selected original table randomness definitions pass,
  including `test_table_randomness_table_gen`. BlockMatrix is excluded.
  Original behavioral UID assertions remain; only the proven IR-shape assertions
  are adapted away, with the spans recorded in the run manifests.
- `reservoir-next-unsupported.log`: the original expression reservoir-sampling
  test gets past Table._generate but still fails at missing
  `hl.agg._reservoir_sample`; this separate feature is unchanged.

Reproduce the focused run from workspace root:

```sh
PYTHONPATH=/Users/danielking/.codex/worktrees/d453/vail/vail/python \
/private/tmp/vail-main-upstream-env/bin/python -m pytest \
  vail/python/tests/test_table_generate.py vail/python/tests/test_expression_inputs.py \
  vail/python/tests/test_map_partitions_streams.py vail/python/tests/test_randomness.py -q
```

The upstream run uses `vail/conformance/full_hail_pytest.py` with the same absolute
PYTHONPATH, `--source hail/hail/python/test/hail`, `--module table/test_table.py`,
`--selection <this-directory>/selection.json`, `--per-test-timeout 60`, and a new
`--output` directory.

Review history: the initial source audit cleared the implementation. Review
requested removal of callback-count spies; those were removed in the revision
above while preserving all semantic oracles. The fresh focused run passed 99
tests. The upstream run predates this test/docstring-only cleanup; executable
implementation and native artifact are unchanged.

Final review: **APPROVED within the documented scope** for exact revision
`861320e7b99e2ee9d412c8019050bf928c61d191` by required reviewer chat
`01a14128-21b8-7a53-b399-4ad8f9b31f4f`. All findings are closed
(`review.json`). Main was not updated.
