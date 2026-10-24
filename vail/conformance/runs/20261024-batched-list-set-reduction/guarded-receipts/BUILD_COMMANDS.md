Stable checkout /Users/danielking/.codex/worktrees/encoded-list-reduction/vail/vail; target /Users/danielking/.codex/worktrees/post-scan-compat-integration/vail/vail/target. RUSTC_WRAPPER=kache, CARGO_INCREMENTAL=0, CARGO_BUILD_JOBS=2, RUSTFLAGS='-C target-cpu=native' for every build. No dynamic_lookup flag graph.

Normal wheel at exact9213c594:
/private/tmp/vail-encoded-projection-env/bin/maturin build --release --locked --interpreter /private/tmp/vail-encoded-main-integration-env/bin/python --out /private/tmp/h003-list-set-gate/guarded-wheels

Normal installation (replaces the prior rejected wheel through uv, no native transplantation):
uv pip install --python /private/tmp/h003-list-set-gate/candidate-env/bin/python --force-reinstall --no-deps /private/tmp/h003-list-set-gate/guarded-wheels/vail-0.1.0a1-cp311-abi3-macosx_11_0_arm64.whl

Public example, first9213 then example-only5af3779a (identical production):
MACOSX_DEPLOYMENT_TARGET=11.0 PYO3_PYTHON=/private/tmp/vail-encoded-main-integration-env/bin/python cargo build --release --locked -p vail-python -p vail-execution --features pyo3/extension-module --example list_set_hidden_parent

Public Rust tests on5af3779a (same production/test body as9213):
MACOSX_DEPLOYMENT_TARGET=11.0 cargo test --release --locked -p vail-execution --lib list_aggregate::compact::tests --no-run
/Users/danielking/.codex/worktrees/post-scan-compat-integration/vail/vail/target/release/deps/vail_execution-2afe74cecca73bd0 list_aggregate::compact::tests --nocapture

The normal wheel took2m37s, first public example50.01s, owner-fixture-only rebuild3.38s, test executable50.10s. Dependency metadata/build scripts were replayed but cached graphs avoided cold Arrow-array compilation. Disk was monitored; two completed owned wheel archives were removed only after recording their hashes and retaining installed natives. Prior rejected candidate wheel remains. The wheel/native and public test/example artifact graphs are separately identified, not represented as one binary.
