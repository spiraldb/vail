#!/bin/bash
set -euo pipefail
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
root=/home/danielking/h086-direct-call-index
src=/home/danielking/parallel-vcf-export/source/vail
mkdir -p "$root/results" "$root/wheels-baseline"
tar -xf /home/danielking/h086-baseline-source.tar -C /home/danielking/parallel-vcf-export/source
uv venv --python /home/danielking/parallel-vcf-export/env/bin/python "$root/baseline-env"
uv pip install --python "$root/baseline-env/bin/python" numpy pyarrow pytest
export CARGO_BUILD_JOBS=16 CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/home/danielking/entries-count-candidate/target
export RUSTFLAGS='-C target-cpu=native -C link-arg=-fuse-ld=lld'
cd "$src"
/home/danielking/parallel-vcf-export/env/bin/maturin build --release --locked --interpreter "$root/baseline-env/bin/python" --out "$root/wheels-baseline"
uv pip install --python "$root/baseline-env/bin/python" "$root"/wheels-baseline/*.whl
sha256sum "$root"/baseline-env/lib/python*/site-packages/vail/_native.abi3.so > "$root/results/baseline-native.sha256"
sha256sum crates/vail-execution/src/calls.rs crates/vail-execution/src/expressions.rs > "$root/results/baseline-source.sha256"
printf 'ready\n' > "$root/results/baseline-build-complete"
