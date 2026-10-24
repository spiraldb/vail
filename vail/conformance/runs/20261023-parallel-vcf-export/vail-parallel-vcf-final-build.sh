#!/bin/bash
set -euo pipefail
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
b=/home/danielking/parallel-vcf-export
export CARGO_BUILD_JOBS=16 CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/home/danielking/entries-count-candidate/target
export RUSTFLAGS='-C target-cpu=native -C link-arg=-fuse-ld=lld'
uv venv --python "$b/env/bin/python" "$b/baseline-env"
uv pip install --python "$b/baseline-env/bin/python" numpy pyarrow
mkdir -p "$b/baseline-wheels"
tar -xzf /home/danielking/vail-parallel-vcf-baseline.tar.gz -C "$b/source"
cd "$b/source/vail"
"$b/env/bin/maturin" build --release --locked --interpreter "$b/baseline-env/bin/python" --out "$b/baseline-wheels"
uv pip install --python "$b/baseline-env/bin/python" "$b"/baseline-wheels/*.whl
sha256sum "$b"/baseline-env/lib/python*/site-packages/vail/_native.abi3.so > "$b/results/baseline-native-sha256.txt"
sha256sum crates/vail-table/src/vcf_export.rs crates/vail-table/src/compressed_text.rs > "$b/results/baseline-source-sha256.txt"
tar -xzf /home/danielking/vail-parallel-vcf-final.tar.gz -C "$b/source"
"$b/env/bin/maturin" build --release --locked --interpreter "$b/env/bin/python" --out "$b/wheels"
uv pip install --reinstall-package vail --python "$b/env/bin/python" "$b"/wheels/*.whl
sha256sum "$b"/env/lib/python*/site-packages/vail/_native.abi3.so > "$b/results/native-sha256.txt"
sha256sum crates/vail-table/src/vcf_export.rs crates/vail-table/src/compressed_text.rs python/tests/test_vcf_export_native.py > "$b/results/candidate-source-sha256.txt"
"$b/env/bin/python" -m pytest python/tests/test_vcf_export_native.py -q > "$b/results/vcf-final-tests.log" 2>&1
printf '%s\n' ready > "$b/results/final-build-complete"
