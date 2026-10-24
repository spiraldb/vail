#!/bin/bash
set -euo pipefail
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
root=/home/danielking/h086-direct-call-index
src=/home/danielking/parallel-vcf-export/source/vail
while ! test -f "$root/results/baseline-measure-complete"; do sleep 5; done
mkdir -p "$root/wheels-candidate"
tar -xf /home/danielking/h086-candidate-source.tar -C /home/danielking/parallel-vcf-export/source
uv venv --python "$root/baseline-env/bin/python" "$root/candidate-env"
uv pip install --python "$root/candidate-env/bin/python" numpy pyarrow pytest
export CARGO_BUILD_JOBS=16 CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/home/danielking/entries-count-candidate/target
export RUSTFLAGS='-C target-cpu=native -C link-arg=-fuse-ld=lld'
cd "$src"
/home/danielking/parallel-vcf-export/env/bin/maturin build --release --locked --interpreter "$root/candidate-env/bin/python" --out "$root/wheels-candidate"
uv pip install --python "$root/candidate-env/bin/python" "$root"/wheels-candidate/*.whl
sha256sum "$root"/candidate-env/lib/python*/site-packages/vail/_native.abi3.so > "$root/results/candidate-native.sha256"
sha256sum crates/vail-execution/src/calls.rs crates/vail-execution/src/expressions.rs > "$root/results/candidate-source.sha256"
"$root/candidate-env/bin/python" -m pytest -q python/tests/test_call_index.py python/tests/test_call_value.py python/tests/test_genotype_index_call.py > "$root/results/candidate-public-tests.log" 2>&1
cargo test --release --locked -p vail-execution --lib direct_call_index -- --nocapture > "$root/results/candidate-rust-tests.log" 2>&1
cargo test --release --locked -p vail-execution --test call_allocations direct_call_index -- --nocapture > "$root/results/candidate-allocation-test.log" 2>&1
printf 'ready\n' > "$root/results/candidate-build-complete"
