# Shared row aggregation integration

Clean assembly `8e6c26eb288e897f7e74a632b0cf378c62b1aa46` integrates candidate `20584888c432a3130d977e7e2d20c2c4ad8329d9` atop main `a0996a304a6c505de76dc66b32c32f59bdb07831`. No interaction edits were needed. Production delta is the reviewed Python MatrixTable planning change; the Rust/Cargo/patch trees are byte-equivalent to main.

Required review cleared isolated source/test/evidence and exact combined source assembly. Exact evidence/runtime acceptance is requested on this evidence commit.

## Runtime gate

**215 public checks passed**, no failures/skips, covering shared row fractions, empty/sparse/all-missing identities, CSE and callback DAGs/binding, aggregate composition/execution/numeric kernels, scalar extrema, matrix layout/projection and the full RNG suite. `public-tests.log` records the result. Runtime durations were collected while an unrelated normal Rust build was active and are not performance evidence.

Because this integration changes only Python planning and Rust/Cargo is unchanged, the explicitly accepted validation method loads the already installed normal main native extension **in place**, then imports this assembly's Python source. No extension is copied, linked, cached or installed as a workaround. `validate.py` asserts the Rust/Cargo diff is empty and native hash matches before loading; `provenance.json` records exact Python origin, native origin and source hashes.

Native origin: `/Users/danielking/.codex/worktrees/entry-fixes-integration/vail/vail/python/vail/_native.abi3.so`, normally built through kache/maturin for main a0996a3 production. SHA256: `77b0c38a0f7bea5308f1651114d0bc8391d2da93beed0b0f4111bb01e7bd7d49`.

Command:

```sh
/private/tmp/vail-entry-fixes-integration-env/bin/python conformance/runs/20261023-row-sharing-integration/validate.py -q python/tests/test_row_fraction_shared_contexts.py python/tests/test_reduction_expression_sharing.py python/tests/test_array_agg_coordinate_callbacks.py python/tests/test_array_agg_coordinate_dag.py python/tests/test_bind.py python/tests/test_agg_composition_execution.py python/tests/test_aggregate_execution.py python/tests/test_scalar_extrema.py python/tests/test_agg_numeric.py python/tests/test_matrix_layout.py python/tests/test_entry_projection.py python/tests/test_randomness.py --durations=8
```

No new performance claim. Keep isolated benchmark source/artifact attribution and the original profile/timing limitations intact.
