# Normal local validation commands

Exclusive stable checkout:
`/Users/danielking/.codex/worktrees/encoded-list-reduction/vail/vail`.
It was clean and source-asserted at each recorded candidate. Final source is
`00a8d7d7fdefb8d2c324fc33c1ec6c58706f02ec`.

Common environment:

```sh
export RUSTFLAGS='-C target-cpu=native'
export RUSTC_WRAPPER=kache
export CARGO_INCREMENTAL=0
export CARGO_BUILD_JOBS=2
export CARGO_TARGET_DIR=/Users/danielking/.codex/worktrees/post-scan-compat-integration/vail/vail/target
export MACOSX_DEPLOYMENT_TARGET=11.0
export PYO3_PYTHON=/private/tmp/vail-encoded-main-integration-env/bin/python
nice -n 10 cargo test --release --locked -p vail-table -p vail-execution --test owning_coordinate_sum
nice -n 10 /private/tmp/vail-encoded-projection-env/bin/maturin build --release --locked \
  --interpreter "$PYO3_PYTHON" --out /private/tmp/vail-owning-coordinate-gate/whole-bitmap-wheels
uv venv --python "$PYO3_PYTHON" /private/tmp/vail-owning-coordinate-gate/whole-bitmap-env
uv pip install --python /private/tmp/vail-owning-coordinate-gate/whole-bitmap-env/bin/python \
  /private/tmp/vail-owning-coordinate-gate/whole-bitmap-wheels/vail-0.1.0a1-cp311-abi3-macosx_11_0_arm64.whl pytest
nice -n 10 python3 /private/tmp/vail-owning-coordinate-gate/run-public-gate.py \
  /private/tmp/vail-owning-coordinate-gate/whole-bitmap-env/bin/python
```

The saved public driver fixes BLAS/OMP thread counts to1 for correctness-gate
consistency and records every test path. It uses installed candidate imports,
not PYTHONPATH. Maturin supplies its normal platform extension linking flags.

Counterfactual used exactly `c01cfc69` (533 production plus the new regression),
same Rust command with filter
`short_separated_selected_spans_preserve_every_valid_owner`. It failed as
recorded. Corrected complete Rust gate has no test filter. No performance
benchmark, VM startup, perf experiment or upstream dependency change occurred.
